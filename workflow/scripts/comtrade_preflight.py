"""Preflight: counts rows, records the vintage, then writes the download plan.

Everything this script does goes through Comtrade's PUBLIC endpoints, with no
key and off premium quota:

  - previewFinalData(..., countOnly=True) returns the REAL count, not capped.
    The "limited to 500 records" in the README applies only to data rows, not
    to the scalar. Checked: France 2015, all products, gives 674,847, the same
    as totalRecords from the availability endpoint.
  - _getFinalDataAvailability gives classification, totalRecords, checksum and
    last release date per (reporter, period).

So planning the packing optimally costs nothing. Only the download spends
quota.

Public endpoint limit: one period per call
(`{"error":"Maximum number of periods for preview is 1"}`), hence one call per
period. It does not matter, since it is free.
"""

# No `from __future__ import annotations` here: Snakemake inserts a preamble
# before this file, and a __future__ import has to be on the first line.
# Python 3.12 handles `str | None` natively.
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import comtradeapicall
from snakemake.script import snakemake

sys.path.insert(0, str(Path(__file__).resolve().parent))
from comtrade_common import (  # noqa: E402
    FIXED,
    bisect_codes,
    call,
    caps,
    join as _join,
    pack_periods,
    params_hash,
    selection,
)

logging.basicConfig(
    filename=snakemake.log[0],
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def count_rows(params: dict, period: str, cmd: list[str] | None = None) -> int:
    """Free count, one period, through the public endpoint."""
    fn = (comtradeapicall.previewFinalData if params["data_type"] == "final"
          else comtradeapicall.previewTarifflineData)
    df = call(fn, label=f"count {period}", countOnly=True,
              **selection(params, period, cmd))
    return int(df["count"].iloc[0])


def vintage(params: dict, periods: list[str], chunk: int = 10
            ) -> tuple[list[dict], str]:
    """Vintage per (reporter, period): which version of the data we are pulling.

    Comtrade rewrites history. France 2020 was published on 2021-05-26, then
    revised on 2026-05-23. Without this record, two runs of the same parameters
    that diverge are inexplicable. `datasetChecksum` is the strongest anchor:
    unchanged means identical data.
    """
    fn = (comtradeapicall._getFinalDataAvailability if params["data_type"] == "final"
          else comtradeapicall._getTarifflineDataAvailability)
    keep = ["period", "reporterCode", "reporterISO", "classificationCode",
            "totalRecords", "datasetChecksum", "firstReleased", "lastReleased"]
    rows: list[dict] = []
    for i in range(0, len(periods), chunk):
        batch = ",".join(periods[i:i + chunk])
        try:
            df = call(fn, FIXED["type"], params["freq"],
                      params["classification"], batch,
                      _join(params["reporter"]),
                      attempts=3, label=f"availability {batch}")
        except RuntimeError as exc:
            logging.warning("vintage unavailable for %s: %s", batch, exc)
            return rows, "incomplete"
        present = [c for c in keep if c in df.columns]
        rows.extend(df[present].astype(str).to_dict("records"))
    return rows, "complete"


def build_plan(params: dict) -> dict:
    periods = params["period"]
    warn_rows, fail_rows = caps(params)
    warnings: list[str] = []
    calls = 0

    counts: dict[str, int] = {}
    for period in periods:
        counts[period] = count_rows(params, period)
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
            for codes, n in bisect_codes(
                params["cmd"], lambda cs: count_rows(params, period, cs), warn_rows
            ):
                packs.append({"periods": [period], "cmd": codes,
                              "expected_rows": n})
                calls += 1
        else:
            packs.append({"periods": pack, "cmd": None,
                          "expected_rows": expected})

    rows, status = vintage(params, periods)

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


plan = build_plan(snakemake.params["params"])

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
