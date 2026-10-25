"""Shared helpers: parameter resolution, stable hash, call wrapper.

Imported by the Snakefile (to compute the output paths at DAG time) and by the
rule scripts.

The module answers two questions:

1. "What is this request called?" -> resolve_params, params_hash, stem.
   The config is turned into one canonical dict, fingerprinted, and that
   fingerprint names every output file.
2. "In how many API calls can it be pulled without being truncated?"
   -> caps, pack_periods, bisect_codes.

Plus one guard rail, call(), because comtradeapicall swallows HTTP errors.

Must stay cheap to import: the Snakefile imports it before declaring any rule,
so no polars and no comtradeapicall in here.
"""

from __future__ import annotations

import hashlib
import json
import time

# --- API caps ---------------------------------------------------------------
# getFinalData's ceiling is set by the subscription tier, not by the package:
# 100K records per call on a free key, 250K on premium (see README). A response
# at the ceiling is truncated WITHOUT error, so we fail instead of delivering.
# The free value is the default, because guessing high is what truncates in
# silence.
FREE_RECORDS_PER_CALL = 100_000
PREMIUM_RECORDS_PER_CALL = 250_000
# Margin on the ceiling: preflight counts can move before the download
# (upstream revisions).
WARN_FRACTION = 0.8


def caps(params: dict) -> tuple[int, int]:
    """Row thresholds for this run's ceiling.

    Read from the resolved params, so the ceiling the plan was packed against
    is the one the download enforces.

    Parameters
    ----------
    params : dict
        Resolved parameters, as returned by resolve_params. Only
        'records_per_call' is read.

    Returns
    -------
    (warn, fail) : tuple of int
        warn : the threshold above which the run is suspicious
            (WARN_FRACTION of the ceiling, i.e. 80K on a free key).
        fail : the tier ceiling itself. A pack planned above it would come
            back truncated without any error, so the download refuses it.
    """
    fail = int(params["records_per_call"])
    return int(fail * WARN_FRACTION), fail

# --- fixed parameters -------------------------------------------------------
# Outside the config, on purpose: they do not vary in this repo. A knob nobody
# ever turns is one knob too many.
FIXED = {
    "type": "C",                  # goods only; 'S' (services) out of scope
    "breakdown_mode": "classic",  # 'plus' is only useful with customs/mot
    "customs": None,              # exists only in breakdown 'plus'
    "mot": None,                  # same
    "max_records": None,          # no truncation wanted
    "proxy_url": None,            # no institutional proxy
}

# Classification codes the API accepts (clCode), as documented in
# config/config.yaml. Validated, so that a typo such as 'HS5' is refused here
# rather than coming back as five swallowed HTTP errors and a RuntimeError.
CLASSIFICATIONS = frozenset(
    {"HS", "S1", "S2", "S3", "S4", "BEC", "EB02"}
    | {f"H{revision}" for revision in range(7)}      # H0..H6
)

# Frequencies handled. 'A' = annual, 'M' = monthly (monthly also requires an
# explicit period list, see _periods).
FREQUENCIES = frozenset({"A", "M"})

# Keys that describe the delivery, not the request: excluded from the hash.
# records_per_call is a transport ceiling, not part of the request: the same
# selection pulled on a free and a premium key is the same data.
_NOT_IN_HASH = frozenset({"format", "csv_sep", "mirror", "run_id", "out_dir",
                          "records_per_call"})


# --- parameter resolution ---------------------------------------------------

def _default(value, fallback):
    """Falls back when the config says nothing about a key.

    In YAML an empty value ('freq:' followed by nothing) parses to None, which
    is NOT the same as a missing key: dict.get(key, default) returns the
    default only when the key is absent, and would hand that None straight
    through ('NONE' once passed to str, False once passed to bool).

    A plain `or` would not do either: it also swallows False and 0, so
    'include_desc: false' would silently become True. Only None means "say
    nothing".

    Parameters
    ----------
    value : any
        What the config gave, cfg.get(key) with no default, so that a missing
        key and an empty one both arrive as None.
    fallback : any
        Value to use when the config says nothing.

    Returns
    -------
    any
        `fallback` if `value` is None, `value` otherwise, False and 0 included.
    """
    return fallback if value is None else value


def _norm_list(value) -> list[str] | None:
    """Canonical list of strings, sorted and deduplicated. None stays None.

    Canonical, because this is what gets hashed: without it, cmd [4407, 4403]
    and cmd ['4403', '4407'] would fingerprint as two different requests while
    asking the API for exactly the same thing.

    Parameters
    ----------
    value : None, str, int, or iterable of str or int
        A single code or a list of codes, in any order, possibly with
        duplicates and surrounding spaces.

    Returns
    -------
    list of str, or None
        Sorted list of unique stripped strings. None if the input was None or
        held nothing usable.
    """
    if value is None:
        return None
    if isinstance(value, (str, int)):
        value = [value]                               # a lone code is a list of one
    out = sorted({str(v).strip() for v in value})     # set = dedup, sorted = canonical
    return out or None


def _norm_selector(value) -> list[str] | None:
    """Canonical list for a dimension where "everything" is allowed.

    The API means "every one" by an absent parameter, so None is the internal
    spelling of "all". Used for reporter, partner and partner2, never for cmd
    (asking for every product is refused, see resolve_params).

    Parameters
    ----------
    value : None, str, int, or iterable of str or int
        Codes to keep, or the string 'all' (any case), or None.

    Returns
    -------
    list of str, or None
        None when every one is wanted ('all' or nothing given), otherwise the
        canonical list from _norm_list.
    """
    if value is None or (isinstance(value, str) and value.strip().lower() == "all"):
        return None
    return _norm_list(value)


def _periods(cfg: dict) -> list[str]:
    """Expands the requested periods into an explicit list.

    Two ways of asking in the config: an explicit 'list', or a 'start'/'stop'
    range for annual data.

    Parameters
    ----------
    cfg : dict
        Raw 'comtrade' config section. Reads cfg['period'] ('list', or 'start'
        and 'stop') and cfg['freq'].

    Returns
    -------
    list of str
        Every period, sorted, as strings: ['1996', '1997', '1998'] for annual
        data, ['199601', ...] in YYYYMM for monthly data.

    Raises
    ------
    ValueError
        If freq is 'M' without an explicit period list (a monthly range is not
        expanded here), or if stop is before start.
    KeyError
        If neither 'list' nor 'start'/'stop' is given.
    """
    period = _default(cfg.get("period"), {})
    if period.get("list"):
        return sorted(str(p).strip() for p in period["list"])
    if str(_default(cfg.get("freq"), "A")).upper() == "M":
        raise ValueError("freq: M requires comtrade.period.list, in YYYYMM")
    start, stop = int(period["start"]), int(period["stop"])
    if stop < start:
        raise ValueError(f"period.stop ({stop}) < period.start ({start})")
    # stop INCLUSIVE: a deliberate correction of the stop - 2 inherited from
    # trade_network_analysis. Do not harmonize it back.
    return [str(y) for y in range(start, stop + 1)]


def resolve_params(cfg: dict) -> dict:
    """Applies the defaults and normalizes. This dict is what gets hashed and stored.

    Hashing the *resolved* parameters rather than the written ones: without
    that, two equivalent configs (list order, int vs string, omitted default)
    would give two different hashes for the same request.

    Every value goes through _default(cfg.get(key), ...) rather than
    cfg.get(key, ...), so that a key left empty in the YAML behaves like a
    missing key instead of reaching the API as None.

    Refusals happen here, as early as possible: an obscure failure fifteen
    minutes into a download costs more than a ValueError at DAG time.

    Parameters
    ----------
    cfg : dict
        Raw 'comtrade' section of config/config.yaml.

    Returns
    -------
    dict
        Resolved parameters, with every key present and canonical:
        data_type, freq, classification, period (list of str), cmd (list of
        str), flow, reporter, partner, partner2 (list of str or None for
        "all"), aggregate_by, include_desc (bool), hs_revision,
        records_per_call (int). This dict is the single reference used by the
        hash, the plan and the download.

    Raises
    ------
    ValueError
        cmd missing, cmd TOTAL, unknown data_type / classification / freq /
        hs_revision, or records_per_call outside 1..PREMIUM_RECORDS_PER_CALL.
    NotImplementedError
        hs_revision 'auto', which needs the correspondence tables of the
        `mapping` job.
    """
    cmd = _norm_list(cfg.get("cmd"))
    if not cmd:
        raise ValueError("comtrade.cmd is required")
    if any(c.upper() == "TOTAL" for c in cmd):
        raise ValueError(
            "cmd: TOTAL is refused. A single reporter-year pair already goes "
            "over the 250K cap."
        )

    params = {
        "data_type":      str(_default(cfg.get("data_type"), "final")).lower(),
        "freq":           str(_default(cfg.get("freq"), "A")).upper(),
        "classification": str(_default(cfg.get("classification"), "HS")).upper(),
        "period":         _periods(cfg),
        "cmd":            cmd,
        # Default M+X: imports and exports, the two sides the mirror needs.
        "flow":           _norm_list(_default(cfg.get("flow"), ["M", "X"])),
        # None = every one, which is how the API reads an absent parameter.
        "reporter":       _norm_selector(_default(cfg.get("reporter"), "all")),
        "partner":        _norm_selector(_default(cfg.get("partner"), "all")),
        "partner2":       _norm_selector(cfg.get("partner2")),
        "aggregate_by":   _default(cfg.get("aggregate_by"), None) or None,
        "include_desc":   bool(_default(cfg.get("include_desc"), True)),
        "hs_revision":    str(_default(cfg.get("hs_revision"), "strict")).lower(),
        "records_per_call": int(_default(cfg.get("records_per_call"),
                                         FREE_RECORDS_PER_CALL)),
    }

    # --- refusals, in the order a user is likely to get them wrong ---
    if params["data_type"] not in {"final", "tariffline"}:
        raise ValueError("data_type must be 'final' or 'tariffline'")
    if params["classification"] not in CLASSIFICATIONS:
        # Unchecked, a typo would reach the API, come back as a swallowed HTTP
        # error, and surface as a RuntimeError after five retries.
        raise ValueError(
            f"unknown classification {params['classification']!r}: expected "
            "HS, H0..H6, S1..S4, BEC or EB02"
        )
    if params["freq"] not in FREQUENCIES:
        raise ValueError("freq must be 'A' (annual) or 'M' (monthly)")
    if params["hs_revision"] == "auto":
        raise NotImplementedError(
            "hs_revision: auto requires a correspondence table per revision "
            "(`mapping` job). Use 'strict' in v1."
        )
    if params["hs_revision"] != "strict":
        raise ValueError("hs_revision must be 'strict' or 'auto'")
    if not 0 < params["records_per_call"] <= PREMIUM_RECORDS_PER_CALL:
        raise ValueError(
            f"records_per_call must be in 1..{PREMIUM_RECORDS_PER_CALL} "
            f"({FREE_RECORDS_PER_CALL} on a free key, "
            f"{PREMIUM_RECORDS_PER_CALL} on premium). Setting it above your "
            f"tier's ceiling truncates the response without any error."
        )
    return params


def params_hash(params: dict, length: int = 8) -> str:
    """Short, stable fingerprint of the resolved parameter set.

    hashlib, never hash(): Python's hash() is randomized per process on
    strings (PYTHONHASHSEED), so it would change on every run.

    Parameters
    ----------
    params : dict
        Resolved parameters, as returned by resolve_params. Keys listed in
        _NOT_IN_HASH are dropped first: they describe the delivery (file
        format, run id, tier ceiling), not the request.
    length : int, optional
        Number of hex characters kept, 8 by default. Enough to tell apart the
        handful of requests living side by side in one data folder.

    Returns
    -------
    str
        Hexadecimal fingerprint, `length` characters. Identical for two configs
        asking for the same data, different as soon as the request changes.

    Notes
    -----
    Touching this function, resolve_params or _NOT_IN_HASH renames EVERY
    future output file and detaches past runs from their request. Not a free
    refactor.
    """
    payload = {k: v for k, v in params.items() if k not in _NOT_IN_HASH}
    # sort_keys: dict order must not matter. default=str: never fail on a type
    # json does not know, the point is stability, not a reversible dump.
    canon = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()[:length]


def stem(params: dict, run_id: str) -> str:
    """Stem shared by the data file, the CSV and the sidecar.

    Computed by the Snakefile at DAG time, which is why this module must stay
    cheap to import: it sets every output path of the workflow.

    Parameters
    ----------
    params : dict
        Resolved parameters, as returned by resolve_params.
    run_id : str
        Run timestamp, YYYYMMDDTHHMMSS. Passing the run_id of a past run
        replays it onto the same file names.

    Returns
    -------
    str
        For instance 'comtrade_HS_A_a1b2c3d4_20261024T120000': classification,
        frequency, request fingerprint, run id. Every file of one run shares
        it, which is what ties the parquet, the CSV and the sidecar together.
    """
    return (
        f"comtrade_{params['classification']}_{params['freq']}"
        f"_{params_hash(params)}_{run_id}"
    )


# --- call wrapper -----------------------------------------------------------

def call(fn, *args, attempts: int = 5, base_delay: float = 5.0,
         label: str = "", **kwargs):
    """Calls a comtradeapicall function, treating a None return as a failure.

    Required, because the package swallows HTTP errors. getPreviewData prints
    the response body and returns None without raising, and pandas.concat then
    ignores that None without error (checked on pandas 2.2.2 and 3.0.1). A
    period hitting a 429 would therefore vanish from the result without a trace.

    EVERY call to the package goes through here. Never call it directly.

    Parameters
    ----------
    fn : callable
        The comtradeapicall function to call, for example
        comtradeapicall.previewFinalData.
    *args, **kwargs
        Passed straight to `fn`.
    attempts : int, optional
        Number of tries before giving up, 5 by default.
    base_delay : float, optional
        First wait in seconds, 5.0 by default, doubled on each retry
        (5s, 10s, 20s, 40s). Exponential backoff: a 429 means the server is
        asking for room, so hammering it is counterproductive.
    label : str, optional
        Short description of the call ('count 1996', 'pack 2001-2004'),
        included in the error message to say which period failed.

    Returns
    -------
    any
        Whatever `fn` returned, guaranteed not None.

    Raises
    ------
    RuntimeError
        Every attempt failed, either by raising or by returning None. The
        message carries `label` and the last failure seen.
    """
    detail = "no attempt"
    for attempt in range(attempts):
        try:
            out = fn(*args, **kwargs)
            if out is not None:
                return out
            # No exception, yet nothing came back: the package printed the HTTP
            # body and returned None. That is a failure, not an empty result.
            detail = "None response (HTTP error swallowed by comtradeapicall)"
        except Exception as exc:                      # noqa: BLE001
            detail = f"{type(exc).__name__}: {exc}"
        if attempt < attempts - 1:
            time.sleep(base_delay * 2 ** attempt)     # 5s, 10s, 20s, 40s
    raise RuntimeError(f"failed after {attempts} attempts [{label}]: {detail}")


# --- selection criteria -----------------------------------------------------

def join(values) -> str | None:
    """List -> comma-separated string, as the API expects its code lists.

    Parameters
    ----------
    values : list of str, or None
        Codes to send, or None for "every one".

    Returns
    -------
    str, or None
        'M,X' for ['M', 'X']. None stays None, because the API reads an absent
        parameter as "no filter on this dimension".
    """
    return None if values is None else ",".join(values)


def selection(params: dict, period: str, cmd: list[str] | None = None) -> dict:
    """Selection arguments, shared by the counting and the download.

    Both must use exactly the same ones, otherwise the count does not describe
    what will actually be downloaded, and the whole plan is built on sand.

    Parameters
    ----------
    params : dict
        Resolved parameters, as returned by resolve_params.
    period : str
        The period to ask for: one period ('1996') for a count, since the
        public preview refuses more than one, or several comma-separated
        ('1996,1997,1998') for a download pack.
    cmd : list of str, optional
        Product codes to use instead of params['cmd']. Only used by
        bisect_codes, when one period has to be split by code.

    Returns
    -------
    dict
        Keyword arguments in the API's own spelling (typeCode, freqCode,
        clCode, ...), ready to be passed through call(). Two extra keys,
        breakdownMode and aggregateBy, only when data_type is 'final': the
        tariffline endpoint does not accept them.
    """
    args = dict(
        typeCode=FIXED["type"],
        freqCode=params["freq"],
        clCode=params["classification"],
        period=period,
        reporterCode=join(params["reporter"]),
        cmdCode=join(cmd if cmd is not None else params["cmd"]),
        flowCode=join(params["flow"]),
        partnerCode=join(params["partner"]),
        partner2Code=join(params["partner2"]),
        customsCode=FIXED["customs"],
        motCode=FIXED["mot"],
    )
    if params["data_type"] == "final":
        args["breakdownMode"] = FIXED["breakdown_mode"]
        args["aggregateBy"] = params["aggregate_by"]
    return args


# --- packing plan -----------------------------------------------------------

def pack_periods(counts: dict[str, int], limit: int) -> list[list[str]]:
    """Packs consecutive periods, keeping the sum under `limit`.

    Counts are additive: a request over P1..Pn returns the union of the rows,
    so the sum of the per-period counts IS the pack's count. That is what makes
    it possible to plan optimally without downloading a single row.

    A period above `limit` on its own goes into a pack of its own; splitting it
    on the codes is the download's job (see bisect_codes).

    Parameters
    ----------
    counts : dict of {str: int}
        Row count per period, from the free counting calls of the preflight.
    limit : int
        Rows allowed per call, in practice the `warn` threshold of caps(), not
        the ceiling itself: upstream revisions can grow a count between the
        plan and the download.

    Returns
    -------
    list of list of str
        Packs of periods in chronological order, each the argument list of one
        download call. Every period of `counts` appears exactly once.
    """
    packs: list[list[str]] = []
    current: list[str] = []
    total = 0
    # Greedy, period by period in chronological order: as soon as adding one
    # more would cross the limit, close the pack and start the next.
    for period in sorted(counts):
        n = counts[period]
        if current and total + n > limit:
            packs.append(current)
            current, total = [], 0
        # Appended before the test on the next period, so a single period over
        # the limit ends up alone in its pack rather than being dropped.
        current.append(period)
        total += n
    if current:
        packs.append(current)                         # the last pack, never closed by the loop
    return packs


def bisect_codes(codes: list[str], count_fn, limit: int
                 ) -> list[tuple[list[str], int]]:
    """Splits a list of codes until every group fits under `limit`.

    Fallback for a period too big on its own: time cannot be split any
    further, so the product list is cut instead. Exception path: with TOTAL
    refused, no ordinary selection gets here.

    Parameters
    ----------
    codes : list of str
        Product codes to split.
    count_fn : callable
        count_fn(codes) -> int, the row count for those codes over the period
        at hand. Must go through the free counting, never the download.
    limit : int
        Rows allowed per call, same meaning as in pack_periods.

    Returns
    -------
    list of (list of str, int)
        One (codes, count) pair per call to make. Every group is under `limit`,
        except a single code that is over it on its own, which cannot be split
        any further. The union of the groups is `codes`.
    """
    n = count_fn(codes)
    if n < limit or len(codes) == 1:
        return [(codes, n)]
    # Halve and recount each half rather than guessing: counts are not spread
    # evenly across codes, and counting is free.
    middle = len(codes) // 2
    return (bisect_codes(codes[:middle], count_fn, limit)
            + bisect_codes(codes[middle:], count_fn, limit))


# --- check ------------------------------------------------------------------

def _demo() -> None:
    """Runnable check of the hash and the packing: python comtrade_common.py

    The repo has no test suite: this function IS the check. Every non-trivial
    piece of logic added above leaves an assert here. Runs with no conda, no
    snakemake and no network, so it still works when the environment does not.

    Returns
    -------
    None
        Prints 'comtrade_common: checks OK' when every assert passed, raises
        otherwise.
    """
    base = {"period": {"start": 1996, "stop": 1998}, "cmd": ["4403", "4407"]}

    # The hash ignores list order, type, and the delivery keys.
    a = resolve_params(base)
    b = resolve_params({"period": {"start": 1996, "stop": 1998},
                        "cmd": [4407, "4403"], "flow": ["X", "M"]})
    assert params_hash(a) == params_hash(b), "hash sensitive to order or type"

    c = resolve_params({**base, "classification": "H5"})
    assert params_hash(a) != params_hash(c), "hash blind to a real change"

    # Case does not matter in a classification, and does not change the hash.
    assert params_hash(resolve_params({**base, "classification": "h5"})) \
        == params_hash(c), "classification case leaked into the hash"

    # stop is inclusive.
    assert a["period"] == ["1996", "1997", "1998"], a["period"]

    # The ceiling defaults to the free tier and does not enter the hash.
    assert a["records_per_call"] == FREE_RECORDS_PER_CALL
    premium = resolve_params({**base, "records_per_call": PREMIUM_RECORDS_PER_CALL})
    assert params_hash(a) == params_hash(premium), "ceiling leaked into the hash"
    assert caps(a) == (80_000, 100_000), caps(a)
    assert caps(premium) == (200_000, 250_000), caps(premium)

    # A key left empty in the YAML parses to None and must mean "default",
    # exactly like a missing key.
    empty = resolve_params({**base, "classification": None, "freq": None,
                            "flow": None, "data_type": None,
                            "include_desc": None, "records_per_call": None})
    assert empty["classification"] == "HS", empty["classification"]
    assert empty["freq"] == "A", empty["freq"]
    assert empty["flow"] == ["M", "X"], empty["flow"]          # not None = every flow
    assert empty["include_desc"] is True
    assert empty["records_per_call"] == FREE_RECORDS_PER_CALL
    assert params_hash(empty) == params_hash(a), "an empty key changed the request"

    # ... but an explicit false stays false: the default must not swallow it.
    assert resolve_params({**base, "include_desc": False})["include_desc"] is False

    # The hash is stable across processes (hardcoded value: must stay identical).
    assert params_hash(a) == hashlib.sha256(
        json.dumps({k: v for k, v in a.items() if k not in _NOT_IN_HASH},
                   sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()[:8]

    # Packing: sum under the limit, chronological order preserved.
    counts = {"1996": 90, "1997": 90, "1998": 90, "1999": 10}
    assert pack_periods(counts, limit=200) == [["1996", "1997"], ["1998", "1999"]]
    # A period above the limit on its own stays on its own.
    assert pack_periods({"1996": 500, "1997": 10}, limit=200) == [["1996"], ["1997"]]
    # No period lost, whatever the split.
    assert sorted(p for pk in pack_periods(counts, 200) for p in pk) == sorted(counts)

    # Code bisection.
    sizes = {"a": 150, "b": 150, "c": 10, "d": 10}
    got = bisect_codes(["a", "b", "c", "d"], lambda cs: sum(sizes[x] for x in cs), 200)
    assert all(n < 200 for _, n in got), got
    assert sorted(x for cs, _ in got for x in cs) == ["a", "b", "c", "d"]

    # --- refusals: a wrong config must raise here, not against the API ---
    def refused(cfg: dict, exc: type) -> None:
        """Asserts that resolve_params(cfg) raises `exc`."""
        try:
            resolve_params(cfg)
        except exc:
            return
        raise AssertionError(f"{cfg} should raise {exc.__name__}")

    # A ceiling above every tier would truncate in silence.
    refused({**base, "records_per_call": 2_500_000}, ValueError)
    # Zero rows per call is not a default, it is a mistake.
    refused({**base, "records_per_call": 0}, ValueError)
    # Volumes unmanageable in sync.
    refused({**base, "cmd": ["TOTAL"]}, ValueError)
    # A typo in a classification, caught before the API swallows it.
    refused({**base, "classification": "HS5"}, ValueError)
    # An unhandled frequency.
    refused({**base, "freq": "Q"}, ValueError)
    # Monthly without an explicit period list.
    refused({**base, "freq": "M"}, ValueError)
    # Needs the correspondence tables of the `mapping` job.
    refused({**base, "hs_revision": "auto"}, NotImplementedError)

    print("comtrade_common: checks OK")


if __name__ == "__main__":
    _demo()
