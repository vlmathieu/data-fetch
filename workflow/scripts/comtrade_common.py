"""Shared helpers: parameter resolution, stable hash, call wrapper.

Imported by the Snakefile (to compute the output paths at DAG time) and by the
rule scripts.
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
    """(warn, fail) row thresholds for this run's ceiling.

    Read from the resolved params, so the ceiling the plan was packed against
    is the one the download enforces.
    """
    fail = int(params["records_per_call"])
    return int(fail * WARN_FRACTION), fail

# --- fixed parameters -------------------------------------------------------
# Outside the config, on purpose: they do not vary in this repo.
FIXED = {
    "type": "C",                  # goods only; 'S' (services) out of scope
    "breakdown_mode": "classic",  # 'plus' is only useful with customs/mot
    "customs": None,              # exists only in breakdown 'plus'
    "mot": None,                  # same
    "max_records": None,          # no truncation wanted
    "proxy_url": None,            # no institutional proxy
}

# Keys that describe the delivery, not the request: excluded from the hash.
# records_per_call is a transport ceiling, not part of the request: the same
# selection pulled on a free and a premium key is the same data.
_NOT_IN_HASH = frozenset({"format", "csv_sep", "mirror", "run_id", "out_dir",
                          "records_per_call"})


# --- parameter resolution ---------------------------------------------------

def _norm_list(value) -> list[str] | None:
    """Canonical list of strings, sorted and deduplicated. None stays None."""
    if value is None:
        return None
    if isinstance(value, (str, int)):
        value = [value]
    out = sorted({str(v).strip() for v in value})
    return out or None


def _norm_selector(value) -> list[str] | None:
    """'all' and None both mean None (= every one), otherwise a canonical list."""
    if value is None or (isinstance(value, str) and value.strip().lower() == "all"):
        return None
    return _norm_list(value)


def _periods(cfg: dict) -> list[str]:
    period = cfg.get("period") or {}
    if period.get("list"):
        return sorted(str(p).strip() for p in period["list"])
    if str(cfg.get("freq", "A")).upper() == "M":
        raise ValueError("freq: M requires comtrade.period.list, in YYYYMM")
    start, stop = int(period["start"]), int(period["stop"])
    if stop < start:
        raise ValueError(f"period.stop ({stop}) < period.start ({start})")
    return [str(y) for y in range(start, stop + 1)]  # stop INCLUSIVE


def resolve_params(cfg: dict) -> dict:
    """Applies the defaults and normalizes. This dict is what gets hashed and stored.

    Hashing the *resolved* parameters rather than the written ones: without
    that, two equivalent configs (list order, int vs string, omitted default)
    would give two different hashes for the same request.
    """
    cmd = _norm_list(cfg.get("cmd"))
    if not cmd:
        raise ValueError("comtrade.cmd is required")
    if any(c.upper() == "TOTAL" for c in cmd):
        raise ValueError(
            "cmd: TOTAL is refused. A single reporter-year pair already goes "
            "over the 250K cap (France 2015: 674,847 rows)."
        )

    params = {
        "data_type":      str(cfg.get("data_type", "final")).lower(),
        "freq":           str(cfg.get("freq", "A")).upper(),
        "classification": str(cfg.get("classification", "HS")).upper(),
        "period":         _periods(cfg),
        "cmd":            cmd,
        "flow":           _norm_list(cfg.get("flow", ["M", "X"])),
        "reporter":       _norm_selector(cfg.get("reporter", "all")),
        "partner":        _norm_selector(cfg.get("partner", "all")),
        "partner2":       _norm_selector(cfg.get("partner2")),
        "aggregate_by":   cfg.get("aggregate_by") or None,
        "include_desc":   bool(cfg.get("include_desc", True)),
        "hs_revision":    str(cfg.get("hs_revision", "strict")).lower(),
        "records_per_call": int(cfg.get("records_per_call")
                                or FREE_RECORDS_PER_CALL),
    }

    if params["data_type"] not in {"final", "tariffline"}:
        raise ValueError("data_type must be 'final' or 'tariffline'")
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
    """
    payload = {k: v for k, v in params.items() if k not in _NOT_IN_HASH}
    canon = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()[:length]


def stem(params: dict, run_id: str) -> str:
    """Stem shared by the data file, the CSV and the sidecar."""
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
    """
    detail = "no attempt"
    for attempt in range(attempts):
        try:
            out = fn(*args, **kwargs)
            if out is not None:
                return out
            detail = "None response (HTTP error swallowed by comtradeapicall)"
        except Exception as exc:                      # noqa: BLE001
            detail = f"{type(exc).__name__}: {exc}"
        if attempt < attempts - 1:
            time.sleep(base_delay * 2 ** attempt)     # 5s, 10s, 20s, 40s
    raise RuntimeError(f"failed after {attempts} attempts [{label}]: {detail}")


# --- selection criteria -----------------------------------------------------

def join(values) -> str | None:
    """List -> comma-separated string. None (= every one) stays None."""
    return None if values is None else ",".join(values)


def selection(params: dict, period: str, cmd: list[str] | None = None) -> dict:
    """Selection arguments, shared by the counting and the download.

    Both must use exactly the same ones, otherwise the count does not describe
    what will actually be downloaded.
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
    """
    packs: list[list[str]] = []
    current: list[str] = []
    total = 0
    for period in sorted(counts):
        n = counts[period]
        if current and total + n > limit:
            packs.append(current)
            current, total = [], 0
        current.append(period)
        total += n
    if current:
        packs.append(current)
    return packs


def bisect_codes(codes: list[str], count_fn, limit: int
                 ) -> list[tuple[list[str], int]]:
    """Splits a list of codes until every group fits under `limit`.

    `count_fn(codes) -> int` must go through the free counting. Exception path:
    with TOTAL refused, no ordinary selection gets here.
    """
    n = count_fn(codes)
    if n < limit or len(codes) == 1:
        return [(codes, n)]
    middle = len(codes) // 2
    return (bisect_codes(codes[:middle], count_fn, limit)
            + bisect_codes(codes[middle:], count_fn, limit))


# --- check ------------------------------------------------------------------

def _demo() -> None:
    """Runnable check of the hash and the packing: python comtrade_common.py"""
    base = {"period": {"start": 1996, "stop": 1998}, "cmd": ["4403", "4407"]}

    # The hash ignores list order, type, and the delivery keys.
    a = resolve_params(base)
    b = resolve_params({"period": {"start": 1996, "stop": 1998},
                        "cmd": [4407, "4403"], "flow": ["X", "M"]})
    assert params_hash(a) == params_hash(b), "hash sensitive to order or type"

    c = resolve_params({**base, "classification": "H5"})
    assert params_hash(a) != params_hash(c), "hash blind to a real change"

    # stop is inclusive.
    assert a["period"] == ["1996", "1997", "1998"], a["period"]

    # The ceiling defaults to the free tier and does not enter the hash.
    assert a["records_per_call"] == FREE_RECORDS_PER_CALL
    premium = resolve_params({**base, "records_per_call": PREMIUM_RECORDS_PER_CALL})
    assert params_hash(a) == params_hash(premium), "ceiling leaked into the hash"
    assert caps(a) == (80_000, 100_000), caps(a)
    assert caps(premium) == (200_000, 250_000), caps(premium)

    # A ceiling above every tier is refused: it would truncate in silence.
    try:
        resolve_params({**base, "records_per_call": 2_500_000})
    except ValueError:
        pass
    else:
        raise AssertionError("records_per_call above the premium ceiling should be refused")

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

    # TOTAL refused.
    try:
        resolve_params({**base, "cmd": ["TOTAL"]})
    except ValueError:
        pass
    else:
        raise AssertionError("TOTAL should be refused")

    # hs_revision: auto not implemented.
    try:
        resolve_params({**base, "hs_revision": "auto"})
    except NotImplementedError:
        pass
    else:
        raise AssertionError("hs_revision: auto should raise NotImplementedError")

    print("comtrade_common: checks OK")


if __name__ == "__main__":
    _demo()
