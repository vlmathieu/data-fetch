"""Preflight: counts rows, records the vintage, then writes the download plan.

Everything this script does goes through Comtrade's PUBLIC endpoints, with no
key and off premium quota:

  - previewFinalData(..., countOnly=True) returns the REAL count, not capped.
    The "limited to 500 records" in the README applies only to data rows, not
    to the scalar.
  - _getFinalDataAvailability gives classification, totalRecords, checksum and
    last release date per (reporter, period).

Planning the packing optimally costs nothing. Only the download spends quota.

Public endpoint limit: one period per call
(`{"error":"Maximum number of periods for preview is 1"}`), hence one call per
period.
"""

# Python 3.12 handles `str | None` natively.
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import comtradeapicall
# `snakemake` is NOT imported: the preamble Snakemake inserts above this file
# defines it as a global. The old `from snakemake.script import snakemake` only
# worked because Snakemake 8's own preamble also set it as a module attribute;
# Snakemake 9 dropped that, so the import raises ImportError there. Relying on
# the injected global works on both. Hence the noqa: F821 on its first uses --
# a linter cannot see the preamble.

sys.path.insert(0, str(Path(__file__).resolve().parent))
from comtrade_common import (  # noqa: E402
    bisect_codes,
    call,
    caps,
    check_packing,
    pack_periods,
    params_hash,
    raise_on_fail,
    selection,
    vintage,
)
from comtrade_progress import Progress, retry_reporter  # noqa: E402

logging.basicConfig(
    filename=snakemake.log[0],  # noqa: F821
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def count_rows(params: dict, period: str, cmd: list[str] | None = None,
               on_retry=None) -> int:
    """Free count, one period, through the public endpoint."""
    fn = (comtradeapicall.previewFinalData if params["data_type"] == "final"
          else comtradeapicall.previewTarifflineData)
    df = call(fn, label=f"count {period}", countOnly=True, on_retry=on_retry,
              **selection(params, period, cmd))
    return int(df["count"].iloc[0])


def build_plan(params: dict, progress: Progress) -> dict:
    periods = params["period"]
    warn_rows, fail_rows = caps(params)
    warnings: list[str] = []
    calls = 0

    counts: dict[str, int] = {}
    for period in periods:
        # Announced BEFORE the call: the whole point is that the display says
        # what is in flight rather than what last finished. `progress` is
        # never None -- a disabled one no-ops, so nothing here has to branch.
        progress.step(f"counting {period}", None)
        counts[period] = count_rows(
            params, period,
            on_retry=retry_reporter(progress, f"count {period}",
                                    logging.warning))
        progress.done(1)
        calls += 1
        logging.info("count %s: %d rows", period, counts[period])
        if counts[period] == 0:
            warnings.append(
                f"period {period}: 0 rows for the requested codes. "
                f"In classification {params['classification']}, a code may not "
                f"exist that year (hs_revision policy: "
                f"{params['hs_revision']})."
            )

    packs: list[dict] = []
    for pack in pack_periods(counts, warn_rows):
        expected = sum(counts[p] for p in pack)
        if len(pack) == 1 and expected >= warn_rows:
            # Exception path: a single period already goes over the margin.
            # We split on the codes, still using the free counting.
            period = pack[0]
            warnings.append(
                f"period {period}: {expected} rows >= {warn_rows}, "
                f"split on the codes."
            )
            progress.note(f"splitting {period} on the codes")
            for codes, n in bisect_codes(
                params["cmd"],
                lambda cs: count_rows(
                    params, period, cs,
                    on_retry=retry_reporter(progress, f"count {period}",
                                            logging.warning)),
                warn_rows,
            ):
                packs.append({"periods": [period], "cmd": codes,
                              "expected_rows": n})
                calls += 1
        else:
            packs.append({"periods": pack, "cmd": None,
                          "expected_rows": expected})

    # No bar of its own: batched ten periods at a time, so the number of calls
    # is not known in advance and a second bar would be noise.
    progress.step("recording the data vintage", None)
    rows, status = vintage(params, periods, comtradeapicall)

    return {
        "params_resolved": params,
        "params_hash": params_hash(params),
        "run_id": snakemake.params["run_id"],
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "thresholds": {"warn_rows": warn_rows, "fail_rows": fail_rows},
        "counts": counts,
        "expected_rows_total": sum(counts.values()),
        "packs": packs,
        "preflight_calls": calls,
        "vintage_status": status,
        "vintage": rows,
        "warnings": warnings,
    }


params = snakemake.params["params"]
periods = params["period"]
with Progress(
    total=len(periods),
    desc=f"Preflight {periods[0]}-{periods[-1]}",
    # No leading space and no scaling: 28 periods is a small number, so
    # '18/28 periods' reads better than '18.0/28.0', and tqdm inverts the rate
    # below one unit per second -- '5.1s/period', not '5.1s/ periods'.
    unit="period",
    unit_scale=False,
) as progress:
    plan = build_plan(params, progress)

# Checks on the plan BEFORE it is written. Everything here cost free calls, so
# this is the one place in the repo where a problem can be caught without
# having spent a thing. An incoherent plan never reaches the disk, so no rule
# downstream can consume it and no quota is burnt on it.
findings = check_packing(plan)
for f in findings:
    log = logging.info if f["status"] == "pass" else logging.warning
    log("check %-18s %-4s %s", f["check"], f["status"], f["detail"])
plan["checks"] = {f["check"]: {k: v for k, v in f.items() if k != "check"}
                  for f in findings}
plan["warnings"] += [f"{f['check']}: {f['detail']}"
                     for f in findings if f["status"] != "pass"]
raise_on_fail(findings, "comtrade_preflight")

logging.info(
    "plan: %d period(s), %d expected row(s), %d download call(s) "
    "(%d counting calls, free)",
    len(plan["counts"]), plan["expected_rows_total"], len(plan["packs"]),
    plan["preflight_calls"],
)
for w in plan["warnings"]:
    logging.warning(w)

Path(snakemake.output[0]).write_text(
    json.dumps(plan, indent=2, ensure_ascii=False), encoding="utf-8"
)
