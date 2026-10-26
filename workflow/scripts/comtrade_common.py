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
import logging
import time
from functools import lru_cache
from pathlib import Path

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
# selection pulled on a free and a premium key is the same data. mirror and
# aggregate_eu add a DERIVED file next to the canonical one without touching
# it, so they do not change the request either.
#
# exclude_pseudo is deliberately NOT here: it changes the rows delivered, so
# two files sharing a stem would otherwise hold different data.
_NOT_IN_HASH = frozenset({"format", "csv_sep", "mirror", "aggregate_eu",
                          "run_id", "out_dir", "records_per_call"})


# --- areas: Comtrade codes, ISO3, and what is not a country -----------------
# resources/comtrade_areas.json is VERSIONED, not fetched: resolve_params runs
# at DAG time, and a fingerprint must not depend on a remote file UNSD can
# rewrite. Refresh it with workflow/scripts/build_comtrade_areas.py.
AREAS_PATH = Path(__file__).resolve().parents[2] / "resources" / "comtrade_areas.json"

# Kinds dropped by exclude_pseudo. 'country' and 'proxy' are kept: the single
# proxy is 490 "Other Asia, nes", which is Taiwan, a reporting economy rather
# than a sum of countries.
PSEUDO_KINDS = frozenset({"aggregate", "territory"})


@lru_cache(maxsize=1)
def _load_areas() -> tuple[dict[str, dict], dict[str, tuple[str, ...]]]:
    """Reads the areas table once, and indexes it both ways.

    stdlib json only: this module is imported by the Snakefile before any rule,
    so it must stay free of polars, pandas and network access.

    Returns
    -------
    (by_code, by_iso3) : tuple of dict
        by_code : {'251': {'code', 'iso3', 'desc', 'kind', ...}}, every area,
            keyed by its numeric code as a string.
        by_iso3 : {'FRA': ('251',), 'BEL': ('56', '58')}, countries and proxies
            only, keyed by ISO3. A tuple because 15 ISO3 map to several codes.

    Raises
    ------
    FileNotFoundError
        The table is missing. Regenerate it with build_comtrade_areas.py.
    """
    try:
        payload = json.loads(AREAS_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"{AREAS_PATH} is missing. Regenerate it with "
            f"`python workflow/scripts/build_comtrade_areas.py` (public "
            f"endpoints, no API key)."
        ) from exc

    by_code: dict[str, dict] = {}
    by_iso3: dict[str, list[str]] = {}
    for area in payload["areas"]:
        code = str(area["code"])
        by_code[code] = area
        # Only selectable entities get an ISO3 entry: an aggregate must not be
        # reachable by writing its ISO3, since exclude_pseudo drops it anyway.
        if area["kind"] in {"country", "proxy"} and area["iso3"]:
            by_iso3.setdefault(area["iso3"], []).append(code)
    return by_code, {iso: tuple(sorted(codes)) for iso, codes in by_iso3.items()}


def pseudo_codes() -> frozenset[str]:
    """Numeric codes of everything that is not a country, as strings.

    Used by comtrade_download to drop those rows. Keyed on codes rather than
    ISO3, because the ISO columns are absent when include_desc is false.

    Returns
    -------
    frozenset of str
        The aggregate and territory codes: '0' (World), '97' (European Union),
        '899' (Areas, nes), '837' (Bunkers)... Excludes '490' (Taiwan).
    """
    by_code, _ = _load_areas()
    return frozenset(code for code, area in by_code.items()
                     if area["kind"] in PSEUDO_KINDS)


def area_codes() -> frozenset[str]:
    """Every area code the table knows, as strings.

    The counterpart of pseudo_codes(), and the reason it exists: the table is
    VERSIONED on purpose, so it drifts. An area UNSD added after the last
    build_comtrade_areas.py run is in neither set -- not in pseudo_codes(), so
    exclude_pseudo leaves it in, and not in the table, so nothing knows what it
    is. It ends up treated as a country without a word. Comparing the delivered
    codes against this set is what makes that drift visible.

    Returns
    -------
    frozenset of str
        All 312 codes of resources/comtrade_areas.json, countries, proxies,
        aggregates and territories alike.
    """
    by_code, _ = _load_areas()
    return frozenset(by_code)


def resolve_areas(values, label: str) -> list[str] | None:
    """Resolves a reporter/partner selection to canonical numeric codes.

    Accepts three forms, so that a config can be read by a human:
    'all', numeric codes ('251'), or ISO3 codes ('FRA', any case). Resolution
    happens BEFORE hashing, so reporter [FRA] and reporter ['251'] are the same
    request and share one fingerprint.

    An ISO3 pointing at several codes resolves to ALL of them. Measured on the
    reference tables: the codes of one ISO3 are disjoint in time (FRA -> 251
    only, while 250 'Metropolitan France' carries no rows; BEL -> 58 for
    1996-1998, then 56), so no row is counted twice, and it is the only way to
    get an unbroken series across a historical split.

    Parameters
    ----------
    values : None, str, int, or iterable
        'all' or None for every area, otherwise codes and/or ISO3 mixed freely.
    label : str
        Name of the parameter being resolved ('reporter', 'partner'), used in
        error messages.

    Returns
    -------
    list of str, or None
        Sorted, deduplicated numeric codes as strings. None means "every one",
        which is how the API reads an absent parameter.

    Raises
    ------
    ValueError
        An unknown numeric code, an unknown ISO3, or a value that is neither.
        Raised at DAG time, so a typo costs nothing: it never reaches the API,
        where it would come back as a swallowed HTTP error.
    """
    if values is None or (isinstance(values, str)
                          and values.strip().lower() == "all"):
        return None
    if isinstance(values, (str, int)):
        values = [values]

    by_code, by_iso3 = _load_areas()
    codes: set[str] = set()
    for raw in values:
        item = str(raw).strip()
        if item.isdigit():
            code = str(int(item))                     # '0251' and '251' are one
            if code not in by_code:
                raise ValueError(
                    f"{label}: unknown Comtrade code {item!r}. See "
                    f"resources/comtrade_areas.csv."
                )
            codes.add(code)
        elif len(item) == 3 and item.isalpha():
            found = by_iso3.get(item.upper())
            if not found:
                raise ValueError(
                    f"{label}: unknown ISO3 {item!r}, or it designates an "
                    f"aggregate that cannot be selected. See "
                    f"resources/comtrade_areas.csv."
                )
            codes.update(found)
        else:
            raise ValueError(
                f"{label}: {item!r} is neither a numeric Comtrade code nor an "
                f"ISO3 code. Use 'all', ['251'] or ['FRA']."
            )
    return sorted(codes, key=int)


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
        str), flow, reporter, partner, partner2 (numeric codes as str, or None
        for "all"), aggregate_by, include_desc (bool), hs_revision,
        records_per_call (int), exclude_pseudo (bool). This dict is the single
        reference used by the hash, the plan and the download.

    Raises
    ------
    ValueError
        cmd missing, cmd TOTAL, unknown data_type / classification / freq /
        hs_revision, an unknown area code or ISO3 in reporter / partner /
        partner2, or records_per_call outside 1..PREMIUM_RECORDS_PER_CALL.
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
        # ISO3 or numeric codes, resolved to codes BEFORE hashing so that
        # [FRA] and ['251'] are one request.
        "reporter":       resolve_areas(_default(cfg.get("reporter"), "all"),
                                        "reporter"),
        "partner":        resolve_areas(_default(cfg.get("partner"), "all"),
                                        "partner"),
        "partner2":       resolve_areas(cfg.get("partner2"), "partner2"),
        "aggregate_by":   _default(cfg.get("aggregate_by"), None) or None,
        "include_desc":   bool(_default(cfg.get("include_desc"), True)),
        "hs_revision":    str(_default(cfg.get("hs_revision"), "strict")).lower(),
        "records_per_call": int(_default(cfg.get("records_per_call"),
                                         FREE_RECORDS_PER_CALL)),
        # Changes the rows delivered, hence IN the hash (see _NOT_IN_HASH).
        "exclude_pseudo": bool(_default(cfg.get("exclude_pseudo"), True)),
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


# --- delivery: what the rules need ------------------------------------------
# resolve_params describes the REQUEST, which is what gets hashed. plan()
# describes the DELIVERY: file formats, separator, output paths. Hence the
# keys read here come from the raw config rather than from params, and they
# are the ones listed in _NOT_IN_HASH.

def _formats(cfg: dict) -> list[str]:
    """File formats to write, parquet first and always present.

    parquet is forced in even when the config does not ask for it: it is the
    canonical artifact (types intact), and it is the only format read
    internally, by comtrade_mirror and comtrade_eu. Without this, asking for
    `format: [csv]` together with `mirror: true` would declare a rule whose
    input file is never produced.

    Parameters
    ----------
    cfg : dict
        Raw config section. Reads cfg['format'], defaulting to ['parquet'].

    Returns
    -------
    list of str
        Lowercased formats, 'parquet' first, no duplicates.
    """
    formats = [str(f).lower() for f in _default(cfg.get("format"), ["parquet"])]
    if "parquet" not in formats:
        formats.insert(0, "parquet")
    return formats


def plan(cfg: dict, run_id: str, out_dir: str = "data/comtrade") -> dict:
    """Everything a rules file needs, derived from one config section.

    Called once at DAG time by rules/comtrade.smk, which unpacks it. Keeping it
    here rather than in the Snakefile has one concrete benefit: the parquet
    forcing and the target list become testable by _demo(), which cannot reach
    into a Snakefile.

    Parameters
    ----------
    cfg : dict
        Raw 'comtrade' section of config/config.yaml.
    run_id : str
        Run timestamp, YYYYMMDDTHHMMSS, shared by every source of one
        invocation.
    out_dir : str, optional
        Directory the files land in, 'data/comtrade' by default.

    Returns
    -------
    dict
        params : the resolved request, as resolve_params returns it
        stem : shared file stem, fingerprint included
        out : out_dir, unchanged
        formats : from _formats()
        csv_sep : separator for the CSV outputs
        targets : every file this source must produce, for `rule all`

    Raises
    ------
    ValueError, NotImplementedError
        Any refusal of resolve_params. They fire HERE, at DAG time, before a
        single row is downloaded.
    """
    params = resolve_params(cfg)
    file_stem = stem(params, run_id)
    formats = _formats(cfg)

    targets = [f"{out_dir}/{file_stem}.{fmt}" for fmt in formats]
    targets.append(f"{out_dir}/{file_stem}.meta.json")
    # Derived files: each adds outputs next to the canonical one without
    # changing it, which is why neither flag is in the params hash.
    if _default(cfg.get("mirror"), False):
        targets += [f"{out_dir}/{file_stem}.mirror.{fmt}" for fmt in formats]
    if _default(cfg.get("aggregate_eu"), False):
        targets += [f"{out_dir}/{file_stem}.eu.{fmt}" for fmt in formats]

    return {
        "params": params,
        "stem": file_stem,
        "out": out_dir,
        "formats": formats,
        "csv_sep": str(_default(cfg.get("csv_sep"), ",")),
        "targets": targets,
    }


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


# --- findings: the shared protocol of every check ---------------------------
# One vocabulary for the whole repo, and it lives HERE rather than in
# comtrade_polars because the checks on the plan are stdlib-only: a dict check
# has no business importing polars, and comtrade_preflight does not import it.
#
# Every check returns findings instead of raising, so that ONE run reports
# everything that is off rather than stopping at the first problem. The caller
# logs them, then calls raise_on_fail().

def finding(check: str, status: str, detail: str, **extra) -> dict:
    """One check result.

    Parameters
    ----------
    check : str
        Short name, used as the key in the sidecar's 'checks' block.
    status : {'pass', 'warn', 'fail'}
        'fail' stops the run before anything is written; 'warn' is recorded
        and the run continues.
    detail : str
        One line, readable in a log without any other context.
    **extra
        Numbers worth keeping in the sidecar ('expected', 'received'...).

    Returns
    -------
    dict
        {'check', 'status', 'detail', ...extra}.
    """
    return {"check": check, "status": status, "detail": detail, **extra}


def raise_on_fail(findings: list[dict], label: str) -> None:
    """Raises if any finding failed, naming every one of them.

    Called BEFORE any file is written, so a non-conforming dataset is never
    delivered. Snakemake deletes the `output:` of a failed job but keeps its
    `log:`, so the diagnosis survives the failure.

    Parameters
    ----------
    findings : list of dict
        Output of check_download, check_mirror or check_eu.
    label : str
        What was checked, for the first line of the message.

    Returns
    -------
    None

    Raises
    ------
    RuntimeError
        At least one finding has status 'fail'. The message lists them all,
        not just the first.
    """
    failed = [f for f in findings if f["status"] == "fail"]
    if failed:
        lines = "\n".join(f"  - {f['check']}: {f['detail']}" for f in failed)
        raise RuntimeError(
            f"{label}: {len(failed)} check(s) failed, nothing written.\n"
            f"{lines}"
        )


# --- vintage: which version of the data are we pulling? ----------------------

VINTAGE_KEEP = ("period", "reporterCode", "reporterISO", "classificationCode",
                "totalRecords", "datasetChecksum", "firstReleased",
                "lastReleased")


def vintage(params: dict, periods: list[str], api, chunk: int = 10
            ) -> tuple[list[dict], str]:
    """Vintage per (reporter, period): which version of data we are pulling.

    Comtrade rewrites history. France 2020 was published on 2021-05-26, then
    revised on 2026-05-23. Without this record, two runs of the same parameters
    that diverge are inexplicable. `datasetChecksum` is the strongest anchor:
    unchanged means identical data.

    Lives here rather than in the preflight script because BOTH rules need it
    -- the preflight to record it, the download to compare against it -- and
    because a `script:` file cannot be imported, so nothing in one is testable.

    Parameters
    ----------
    params : dict
        Resolved request, read for 'data_type', 'freq', 'classification' and
        'reporter'.
    periods : list of str
        Periods to ask about.
    api : module
        The comtradeapicall module. INJECTED, never imported here: this module
        is loaded by the Snakefile before any rule and must stay cheap. Same
        pattern as call() taking the function to call.
    chunk : int, optional
        Periods per availability call, 10 by default.

    Returns
    -------
    (rows, status) : tuple
        rows : list of dict
            One record per (reporter, period), values as strings, holding
            VINTAGE_KEEP's columns that the endpoint actually returned.
        status : {'complete', 'incomplete'}
            'incomplete' as soon as one batch could not be read; the rows
            gathered so far are still returned.
    """
    fn = (api._getFinalDataAvailability if params["data_type"] == "final"
          else api._getTarifflineDataAvailability)
    rows: list[dict] = []
    for i in range(0, len(periods), chunk):
        batch = ",".join(periods[i:i + chunk])
        try:
            # typeCode comes FIRST in this signature; forgetting it only
            # raises at runtime.
            df = call(fn, FIXED["type"], params["freq"],
                      params["classification"], batch,
                      join(params["reporter"]),
                      attempts=3, label=f"availability {batch}")
        except RuntimeError as exc:
            logging.warning("vintage unavailable for %s: %s", batch, exc)
            return rows, "incomplete"
        present = [c for c in VINTAGE_KEEP if c in df.columns]
        rows.extend(df[present].astype(str).to_dict("records"))
    return rows, "complete"


def _vintage_index(rows: list[dict]) -> dict[tuple[str, str], str]:
    """Indexes vintage rows by (period, reporterCode) -> datasetChecksum.

    That key is unique: measured on the plan of 2026-10-26, 681 rows for 681
    distinct keys and 681 distinct checksums.
    """
    return {(row["period"], row["reporterCode"]): row.get("datasetChecksum")
            for row in rows}


def compare_vintage(before: list[dict], after: list[dict]) -> dict:
    """Did Comtrade revise the data between the preflight and the download?

    This is what makes a row-count gap conclusive instead of ambiguous. The
    counts can only ever warn on their own, because a gap is as often a
    legitimate upstream revision as a bug here; the checksum tells the two
    apart.

    Parameters
    ----------
    before : list of dict
        plan['vintage'], as recorded by the preflight.
    after : list of dict
        The same query re-run after the download. Free, public endpoint.

    Returns
    -------
    dict
        'changed'   : list of (period, reporterCode) whose checksum moved,
        'appeared'  : keys present only after,
        'vanished'  : keys present only before,
        'unchanged' : how many keys matched,
        'revised'   : bool, true if anything at all moved.
    """
    old, new = _vintage_index(before), _vintage_index(after)
    shared = set(old) & set(new)
    changed = sorted(k for k in shared if old[k] != new[k])
    return {
        "changed": changed,
        "appeared": sorted(set(new) - set(old)),
        "vanished": sorted(set(old) - set(new)),
        "unchanged": len(shared) - len(changed),
        "revised": bool(changed or set(new) ^ set(old)),
    }


def revision_finding(gap: bool, comparison: dict | None) -> dict:
    """Turns a vintage comparison into the verdict on a row-count gap.

    Parameters
    ----------
    gap : bool
        Whether a count check reported a difference. With no gap there is
        nothing to explain.
    comparison : dict or None
        Output of compare_vintage, or None when the vintage could not be
        re-read.

    Returns
    -------
    dict
        One finding named 'revision':
          - no gap                      -> 'pass', nothing was re-read
          - gap and the vintage moved   -> 'warn', the gap is EXPLAINED
          - gap and the vintage is identical -> 'fail', the bug is OURS
          - gap and the vintage unknown -> 'warn', cannot tell, and saying so
            is more honest than picking a side
    """
    if not gap:
        return finding("revision", "pass",
                       "counts match, so the vintage was not re-read")
    if comparison is None:
        return finding("revision", "warn",
                       "counts differ and the vintage could not be re-read, "
                       "so a revision cannot be told apart from a bug")
    if comparison["revised"]:
        return finding(
            "revision", "warn",
            f"counts differ AND the vintage moved "
            f"({len(comparison['changed'])} checksum(s) changed, "
            f"{len(comparison['appeared'])} appeared, "
            f"{len(comparison['vanished'])} vanished): Comtrade revised "
            f"between the preflight and the download, which explains the gap.",
            **comparison)
    return finding(
        "revision", "fail",
        f"counts differ while the vintage is IDENTICAL "
        f"({comparison['unchanged']} checksum(s) unchanged): same data "
        f"upstream, different number of rows here, so the bug is on our side. "
        f"Re-run the preflight to get fresh counts if you believe otherwise.",
        **comparison)


# --- checks on the plan ------------------------------------------------------
# Everything below is stdlib and runs at PREFLIGHT time, on free calls only.
# That matters: every other check in this repo runs after the keyed call, so a
# plan that was already wrong had cost quota by the time anything noticed. The
# strangest asymmetry the repo had was that comtrade_preflight is the one rule
# safe to run freely, and the one rule whose output nothing verified.


def check_packing(plan: dict) -> list[dict]:
    """Checks a download plan against the request it claims to serve.

    Called by comtrade_preflight BEFORE plan.json is written, so an incoherent
    plan is never put on disk and nothing downstream can consume it.

    Parameters
    ----------
    plan : dict
        The plan as build_plan() assembles it: 'params_resolved', 'counts',
        'expected_rows_total', 'packs' and 'thresholds'.

    Returns
    -------
    list of dict
        Findings 'packing_coverage', 'packing_ceiling' and 'packing_total'.
    """
    params = plan["params_resolved"]
    packs = plan["packs"]
    return (_check_coverage(params, plan["counts"], packs)
            + _check_pack_ceiling(packs, plan["thresholds"])
            + _check_pack_total(packs, plan["expected_rows_total"]))


def _check_coverage(params: dict, counts: dict, packs: list[dict]
                    ) -> list[dict]:
    """Every requested (period, code) pair must be asked for exactly once.

    One invariant standing in for four failures, which is why it is worth more
    than the rest of this module put together:

      - a period missing from the packs is never downloaded;
      - a period in two packs is downloaded twice;
      - a code lost by bisect_codes disappears from the result;
      - two code subsets that overlap duplicate every row they share.

    That last one is the payoff. A code overlap IS caught today, by the
    `duplicates` check on the delivered file -- but only after the quota has
    been spent and with a file to throw away. Here it costs nothing and fires
    before anything goes out.

    Parameters
    ----------
    params : dict
        plan['params_resolved'], read for 'period' and 'cmd'.
    counts : dict of {str: int}
        plan['counts'], one free count per period.
    packs : list of dict
        plan['packs']; a pack's 'cmd' of None means every requested code.

    Returns
    -------
    list of dict
        One finding, 'fail' listing what is uncovered or covered twice.
    """
    asked_periods = list(params["period"])
    asked_cmd = list(params["cmd"])
    problems: list[str] = []

    # The counts drive the packing, so they must describe exactly the request.
    # Same invariant seen from the other end, and cheaper to state here than to
    # debug in a pack list.
    if set(counts) != set(asked_periods):
        missing = sorted(set(asked_periods) - set(counts))
        extra = sorted(set(counts) - set(asked_periods))
        problems.append(f"counts cover the wrong periods "
                        f"(missing {missing}, unexpected {extra})")

    for period in asked_periods:
        # cmd None means "every requested code", which is the normal path.
        requested: list[str] = []
        for pack in packs:
            if period in pack["periods"]:
                requested += list(pack["cmd"]) if pack["cmd"] else asked_cmd

        seen = set(requested)
        duplicated = sorted({c for c in requested if requested.count(c) > 1})
        uncovered = sorted(set(asked_cmd) - seen)
        unexpected = sorted(seen - set(asked_cmd))

        if duplicated:
            problems.append(f"{period}: code(s) requested more than once: "
                            f"{duplicated[:10]}")
        if uncovered:
            problems.append(f"{period}: code(s) in no pack: {uncovered[:10]}")
        if unexpected:
            problems.append(f"{period}: code(s) nobody asked for: "
                            f"{unexpected[:10]}")

    if problems:
        return [finding(
            "packing_coverage", "fail",
            f"{len(problems)} problem(s) in the packing: "
            + "; ".join(problems[:5]),
            problems=problems,
        )]
    return [finding(
        "packing_coverage", "pass",
        f"{len(asked_periods)} period(s) x {len(asked_cmd)} code(s) each "
        f"requested exactly once, over {len(packs)} pack(s)")]


def _check_pack_ceiling(packs: list[dict], thresholds: dict) -> list[dict]:
    """No pack may be planned over the ceiling it is meant to stay under.

    The packing's own promise. One exception is legitimate and documented: a
    single period too big on its own is split on the codes, and bisect_codes
    can hand back a single code that is over the limit and cannot be split any
    further. Such a pack is IRREDUCIBLE, so it warns rather than failing.

    Parameters
    ----------
    packs : list of dict
        plan['packs'].
    thresholds : dict
        plan['thresholds'], with 'warn_rows' and 'fail_rows'.

    Returns
    -------
    list of dict
        One finding. 'fail' when a pack is over the ceiling or over the margin
        while still reducible, 'warn' for the irreducible exception.
    """
    warn_rows, fail_rows = thresholds["warn_rows"], thresholds["fail_rows"]
    over_ceiling: list[str] = []
    reducible: list[str] = []
    irreducible: list[str] = []

    for pack in packs:
        rows = pack["expected_rows"]
        periods, cmd = pack["periods"], pack["cmd"]
        if rows < warn_rows:
            continue
        label = (f"{','.join(periods)} / {len(cmd) if cmd else 'all'} "
                 f"code(s): {rows} rows")
        if rows >= fail_rows:
            over_ceiling.append(label)
        elif len(periods) == 1 and cmd is not None and len(cmd) == 1:
            irreducible.append(label)
        else:
            reducible.append(label)

    if over_ceiling:
        return [finding(
            "packing_ceiling", "fail",
            f"pack(s) planned at or above the {fail_rows} ceiling, so the "
            f"download WILL come back truncated: {over_ceiling}. Said now, "
            f"for free, instead of after the call.", packs=over_ceiling)]
    if reducible:
        return [finding(
            "packing_ceiling", "fail",
            f"pack(s) above the {warn_rows} margin that could still have been "
            f"split: {reducible}. The packing did not do its job.",
            packs=reducible)]
    if irreducible:
        return [finding(
            "packing_ceiling", "warn",
            f"pack(s) above the {warn_rows} margin and irreducible (one "
            f"period, one code): {irreducible}. Nothing left to split; an "
            f"upstream revision could push them over the ceiling.",
            packs=irreducible)]
    return [finding("packing_ceiling", "pass",
                    f"{len(packs)} pack(s) all under the {warn_rows} margin")]


def _check_pack_total(packs: list[dict], expected_total: int) -> list[dict]:
    """The packs must add up to the total the counts gave.

    Severity depends on which path built them, and the distinction is not
    pedantry. On the normal path a pack's expected_rows is pure arithmetic over
    `counts`, so a mismatch is certainly a bug: 'fail'. On the bisect_codes
    path each subset was counted by a SEPARATE API call, and the additivity of
    counts over code subsets has never been measured here, so a small gap is
    not proof of anything: 'warn'.

    Parameters
    ----------
    packs : list of dict
        plan['packs'].
    expected_total : int
        plan['expected_rows_total'], the sum of the per-period counts.

    Returns
    -------
    list of dict
        One finding, 'fail' or 'warn' on a mismatch as described above.
    """
    total = sum(pack["expected_rows"] for pack in packs)
    if total == expected_total:
        return [finding("packing_total", "pass",
                        f"{len(packs)} pack(s) add up to {total} rows",
                        expected=expected_total, planned=total)]
    bisected = any(pack["cmd"] for pack in packs)
    return [finding(
        "packing_total", "warn" if bisected else "fail",
        f"the packs add up to {total} rows, the counts to {expected_total} "
        f"({total - expected_total:+d})"
        + (". The codes were split, so each subset came from its own counting "
           "call and a small gap is not proof of a bug."
           if bisected else
           ". No code split, so this is pure arithmetic over the counts."),
        expected=expected_total, planned=total)]


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

    # --- areas: ISO3 or numeric code, same request ---
    # The whole point: a config can be written for humans without changing the
    # fingerprint of the request. Case and type do not matter either.
    numeric = resolve_params({**base, "reporter": ["250", "251"]})
    for written in (["FRA"], ["fra"], "FRA", ["FRA", "FRA"], ["FRA", 251]):
        got = resolve_params({**base, "reporter": written})
        assert got["reporter"] == ["250", "251"], (written, got["reporter"])
        assert params_hash(got) == params_hash(numeric), f"{written} changed the hash"

    # An ISO3 resolves to EVERY one of its codes, because a historical split
    # needs them all: Belgium is code 58 (Belgium-Luxembourg) until 1998, then
    # code 56. One code alone would leave a three-year hole in 1996-2023.
    # Measured: the codes of one ISO3 are disjoint in time, so nothing is
    # counted twice and the unused code returns no rows.
    assert resolve_areas(["BEL"], "reporter") == ["56", "58"]
    assert resolve_areas(["DEU"], "reporter") == ["276", "280"]

    # 'all' and None both mean "no filter", which the API reads as every one.
    assert resolve_areas("all", "reporter") is None
    assert resolve_areas("ALL", "reporter") is None
    assert resolve_areas(None, "reporter") is None

    # Mixed forms, deduplicated, sorted numerically (not as strings: '56'
    # before '251').
    assert resolve_areas(["FRA", "251", "BEL"], "reporter") \
        == ["56", "58", "250", "251"]

    # Refusals, at DAG time rather than against the API.
    refused_areas = (["XXX"], ["9999"], ["France"], ["F"], ["12AB"])
    for bad in refused_areas:
        try:
            resolve_areas(bad, "reporter")
        except ValueError:
            continue
        raise AssertionError(f"{bad} should be refused")

    # An aggregate is not selectable by ISO3: exclude_pseudo drops it anyway,
    # so letting it in by name would be a trap.
    try:
        resolve_areas(["EUR"], "reporter")          # 97, European Union
    except ValueError:
        pass
    else:
        raise AssertionError("EUR (an aggregate) should not be selectable")

    # --- pseudo codes ---
    pseudo = pseudo_codes()
    assert "0" in pseudo, "World is not an area"
    assert "97" in pseudo, "European Union is an aggregate reporter"
    assert "899" in pseudo, "Areas, nes"
    assert "837" in pseudo, "Bunkers"
    assert "492" in pseudo, "Europe EU, nes, despite carrying Monaco's ISO3"
    assert "490" not in pseudo, "490 is Taiwan, a reporting economy: kept"
    assert "251" not in pseudo and "56" not in pseudo
    assert 0 < len(pseudo) < 312, len(pseudo)

    # --- what belongs in the hash ---
    # exclude_pseudo changes the rows delivered, so it must change the stem.
    assert a["exclude_pseudo"] is True
    kept = resolve_params({**base, "exclude_pseudo": False})
    assert params_hash(a) != params_hash(kept), "exclude_pseudo leaked out of the hash"
    # aggregate_eu only adds a derived file, like mirror: same request.
    assert params_hash(resolve_params({**base, "aggregate_eu": True})) \
        == params_hash(a), "aggregate_eu leaked into the hash"
    assert params_hash(resolve_params({**base, "mirror": False})) \
        == params_hash(a), "mirror leaked into the hash"

    # The hash is stable across processes (hardcoded value: must stay identical).
    assert params_hash(a) == hashlib.sha256(
        json.dumps({k: v for k, v in a.items() if k not in _NOT_IN_HASH},
                   sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()[:8]

    # --- delivery: formats and targets ---
    # Until these moved out of the Snakefile, nothing could check them.
    assert _formats({}) == ["parquet"], "parquet is the default"
    # parquet is forced in, FIRST, even when only csv is asked for: the mirror
    # and the EU aggregation read the parquet and never the CSV.
    assert _formats({"format": ["csv"]}) == ["parquet", "csv"]
    # Case is normalized, and parquet is not duplicated when already there.
    assert _formats({"format": ["PARQUET", "CSV"]}) == ["parquet", "csv"]
    assert _formats({"format": ["csv", "parquet"]}) == ["csv", "parquet"]
    assert _formats({"format": None}) == ["parquet"], "empty key means default"

    run = "20261025T120000"
    delivery = plan({**base, "format": ["parquet", "csv"]}, run)
    assert delivery["stem"] == stem(delivery["params"], run), "stem diverged"
    assert delivery["csv_sep"] == ","
    assert delivery["out"] == "data/comtrade"
    assert plan(base, run, out_dir="elsewhere")["out"] == "elsewhere"

    # The canonical files are always targets; the derived ones only on demand.
    def names(cfg: dict) -> set[str]:
        """Target file names, stripped of their directory and stem."""
        got = plan(cfg, run)
        return {t.rsplit(got["stem"], 1)[-1] for t in got["targets"]}

    plain = {**base, "format": ["parquet", "csv"]}
    assert names(plain) == {".parquet", ".csv", ".meta.json"}, names(plain)
    assert names({**plain, "mirror": True}) == {
        ".parquet", ".csv", ".meta.json", ".mirror.parquet", ".mirror.csv"}
    assert names({**plain, "aggregate_eu": True}) == {
        ".parquet", ".csv", ".meta.json", ".eu.parquet", ".eu.csv"}
    assert names({**plain, "mirror": False, "aggregate_eu": False}) \
        == {".parquet", ".csv", ".meta.json"}, "a derived file slipped in"

    # A derived file must not change the request, so not the stem either.
    assert plan({**plain, "aggregate_eu": True}, run)["stem"] \
        == plan(plain, run)["stem"], "aggregate_eu moved the stem"

    # plan() delegates the refusals: a bad config must not reach the API.
    try:
        plan({**base, "cmd": ["TOTAL"]}, run)
    except ValueError:
        pass
    else:
        raise AssertionError("plan() should delegate the refusals")

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

    # --- checks on the plan ---
    # Plans written by hand, because the real plan.json on disk has ONE pack
    # with cmd: null and therefore exercises neither the multi-pack path nor
    # bisect_codes. The paths that could hide a bug are exactly the ones a real
    # plan does not reach, which is why they get asserts of their own.
    def demo_plan(packs, counts=None, total=None, cmd=("4403", "4407")):
        """A plan carrying only what check_packing reads."""
        counts = {"2000": 100, "2001": 200} if counts is None else counts
        return {
            "params_resolved": {"period": sorted(counts), "cmd": list(cmd)},
            "counts": counts,
            "expected_rows_total": (sum(counts.values()) if total is None
                                    else total),
            "packs": packs,
            "thresholds": {"warn_rows": 80_000, "fail_rows": 100_000},
        }

    def pack(periods, cmd=None, rows=0):
        return {"periods": list(periods), "cmd": cmd, "expected_rows": rows}

    def check_status(findings, name):
        return next(f["status"] for f in findings if f["check"] == name)

    # The normal path: one pack over both periods, every code requested once.
    healthy = demo_plan([pack(["2000", "2001"], None, 300)])
    assert all(f["status"] == "pass" for f in check_packing(healthy)), \
        check_packing(healthy)
    raise_on_fail(check_packing(healthy), "demo")

    # COVERAGE. A period in no pack is never downloaded.
    assert check_status(check_packing(demo_plan([pack(["2000"], None, 100)]),
                                      ), "packing_coverage") == "fail"

    # A period in two packs is downloaded twice: every code of it comes back
    # requested twice, which is what the invariant actually measures.
    doubled = demo_plan([pack(["2000", "2001"], None, 300),
                         pack(["2000"], None, 100)])
    assert check_status(check_packing(doubled), "packing_coverage") == "fail"

    # The bisect path, done right: the two code subsets partition cmd.
    split = demo_plan(
        [pack(["2000"], ["4403"], 60_000), pack(["2000"], ["4407"], 40_000),
         pack(["2001"], None, 200)],
        counts={"2000": 100_000, "2001": 200}, total=100_200)
    assert all(f["status"] in {"pass", "warn"}
               for f in check_packing(split)), check_packing(split)
    assert check_status(check_packing(split), "packing_coverage") == "pass"

    # The bisect path, done wrong. BOTH failures are silent in production:
    # a lost code just disappears from the file, and an overlapping subset
    # duplicates rows in a file whose total still looks plausible.
    lost = demo_plan([pack(["2000"], ["4403"], 60_000),
                      pack(["2001"], None, 200)],
                     counts={"2000": 60_000, "2001": 200})
    assert check_status(check_packing(lost), "packing_coverage") == "fail"
    overlap = demo_plan(
        [pack(["2000"], ["4403", "4407"], 60_000),
         pack(["2000"], ["4407"], 40_000), pack(["2001"], None, 200)],
        counts={"2000": 100_000, "2001": 200}, total=100_200)
    assert check_status(check_packing(overlap), "packing_coverage") == "fail"
    # and this is the one that earns the whole check: today it would only be
    # caught by `duplicates`, after the quota was spent.
    assert "requested more than once" in next(
        f for f in check_packing(overlap) if f["check"] == "packing_coverage"
    )["detail"]

    # A code nobody asked for, and counts describing the wrong periods.
    stray = demo_plan([pack(["2000", "2001"], ["4403", "4407", "9999"], 300)])
    assert check_status(check_packing(stray), "packing_coverage") == "fail"
    wrong_counts = demo_plan([pack(["2000", "2001"], None, 300)],
                             counts={"2000": 100, "2001": 200, "2002": 50})
    assert check_status(check_packing(wrong_counts),
                        "packing_coverage") == "fail"

    # CEILING. Over the ceiling: the download will certainly be truncated.
    over = demo_plan([pack(["2000", "2001"], None, 120_000)],
                     counts={"2000": 120_000, "2001": 0}, total=120_000)
    assert check_status(check_packing(over), "packing_ceiling") == "fail"

    # Over the margin while still splittable: the packing did not do its job.
    lazy = demo_plan([pack(["2000", "2001"], None, 90_000)],
                     counts={"2000": 45_000, "2001": 45_000}, total=90_000)
    assert check_status(check_packing(lazy), "packing_ceiling") == "fail"

    # Over the margin and irreducible (one period, one code): the documented
    # exception, so a warning and not a refusal.
    irreducible = demo_plan(
        [pack(["2000"], ["4403"], 90_000), pack(["2000"], ["4407"], 10_000),
         pack(["2001"], None, 200)],
        counts={"2000": 100_000, "2001": 200}, total=100_200)
    assert check_status(check_packing(irreducible),
                        "packing_ceiling") == "warn"
    raise_on_fail(check_packing(irreducible), "demo")     # must not raise

    # TOTAL. Pure arithmetic on the normal path, so a gap is a bug.
    assert check_status(check_packing(demo_plan(
        [pack(["2000", "2001"], None, 250)])), "packing_total") == "fail"
    # On the bisect path each subset came from its own counting call, so the
    # same gap only warns.
    assert check_status(check_packing(demo_plan(
        [pack(["2000"], ["4403"], 50), pack(["2000"], ["4407"], 40),
         pack(["2001"], None, 200)],
        counts={"2000": 100, "2001": 200})), "packing_total") == "warn"

    # --- vintage comparison ---
    before = [
        {"period": "2000", "reporterCode": "251", "datasetChecksum": "a"},
        {"period": "2001", "reporterCode": "251", "datasetChecksum": "b"},
    ]
    same = compare_vintage(before, before)
    assert same == {"changed": [], "appeared": [], "vanished": [],
                    "unchanged": 2, "revised": False}, same

    moved = compare_vintage(before, [{**before[0], "datasetChecksum": "z"},
                                     before[1]])
    assert moved["revised"] and moved["changed"] == [("2000", "251")], moved
    assert moved["unchanged"] == 1

    appeared = compare_vintage(before, before + [
        {"period": "2002", "reporterCode": "251", "datasetChecksum": "c"}])
    assert appeared["revised"] and appeared["appeared"] == [("2002", "251")]
    assert appeared["changed"] == [], appeared
    vanished = compare_vintage(before, before[:1])
    assert vanished["revised"] and vanished["vanished"] == [("2001", "251")]

    # The verdict on a count gap. This is the promotion of the count checks
    # from warn to fail, and the whole reason the vintage is re-read.
    assert revision_finding(False, None)["status"] == "pass"
    assert revision_finding(True, None)["status"] == "warn"
    assert revision_finding(True, moved)["status"] == "warn"
    # Same vintage upstream, different row count here: the bug is ours.
    assert revision_finding(True, same)["status"] == "fail"
    assert "preflight" in revision_finding(True, same)["detail"], \
        "the fail must name the way out"

    # --- areas: the public accessors agree with each other ---
    all_areas, pseudo_set = area_codes(), pseudo_codes()
    assert pseudo_set < all_areas, "pseudo codes must be a strict subset"
    assert "490" in all_areas and "490" not in pseudo_set, "490 is Taiwan"
    assert "0" in pseudo_set, "World is a pseudo area"

    print("comtrade_common: checks OK")


if __name__ == "__main__":
    _demo()
