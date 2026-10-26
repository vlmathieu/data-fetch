"""Mirror flows: both declarations of one edge, side by side.

A bilateral flow is declared twice, by the exporter (flowCode 'X') and by the
importer ('M'). This script does NOT reconcile them: it puts them side by side,
suffixed `_exp` and `_imp`.

Two rules, inherited from trade_network_analysis and non-negotiable:

  - No reconciliation. No FOB/CIF averaging, no substituting one side for the
    other. The gap between the two declarations is a data quality signal, not
    noise to average away.
  - Nulls stay null, not zero. Sums are guarded: a group that is entirely null
    stays null. Otherwise a non-reporter turns into a reporter declaring zero
    and the diagnostic disappears.
"""

# No `from __future__ import annotations` here: Snakemake inserts a preamble
# before this file, and a __future__ import has to be on the first line.
# Python 3.12 handles `str | None` natively.
import logging
import os
import sys
from pathlib import Path

# `snakemake` is NOT imported: the preamble Snakemake inserts above this file
# defines it as a global. The old `from snakemake.script import snakemake` only
# worked because Snakemake 8's own preamble also set it as a module attribute;
# Snakemake 9 dropped that, so the import raises ImportError there. Relying on
# the injected global works on both. Hence the noqa: F821 on its first uses --
# a linter cannot see the preamble.

os.environ["POLARS_MAX_THREADS"] = str(snakemake.threads)  # noqa: F821
import polars as pl  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from comtrade_polars import (  # noqa: E402
    check_mirror,
    guarded_sum,
    raise_on_fail,
)

logging.basicConfig(
    filename=snakemake.log[0],
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Comtrade's measured quantities, in the order their columns appear.
MEASURES = ("netWgt", "grossWgt", "qty", "altQty",
            "fobvalue", "cifvalue", "primaryValue")

# The exporter and the importer are not the same actor depending on the
# direction of the flow: in an export report, the reporter is the exporter.
ROLES = {
    "X": {"reporterISO": "exporterISO", "reporterDesc": "exporterDesc",
          "partnerISO": "importerISO", "partnerDesc": "importerDesc"},
    "M": {"reporterISO": "importerISO", "reporterDesc": "importerDesc",
          "partnerISO": "exporterISO", "partnerDesc": "exporterDesc"},
}

KEYS = ["period", "cmdCode", "exporterISO", "importerISO"]


def one_side(data: pl.DataFrame, flow_code: str, suffix: str) -> pl.DataFrame:
    """One side of the mirror: the declarations for a single flow direction."""
    rename = {k: v for k, v in ROLES[flow_code].items() if k in data.columns}
    measures = [m for m in MEASURES if m in data.columns]
    side = data.filter(pl.col("flowCode") == flow_code).rename(rename)

    descriptions = [c for c in ("exporterDesc", "importerDesc") if c in side.columns]
    aggregated = side.group_by(KEYS).agg(
        [guarded_sum(m) for m in measures]
        # The label depends on the ISO: first non-null, not a group key.
        + [pl.col(d).drop_nulls().first().alias(d) for d in descriptions]
    )
    return aggregated.rename(
        {c: f"{c}_{suffix}" for c in measures + descriptions}
    )


data = (
    pl.read_parquet(snakemake.input[0])
    .with_columns(pl.col("period").cast(pl.String),
                  pl.col("cmdCode").cast(pl.String))
)

dropped = sorted(set(data["flowCode"].unique()) - {"M", "X"})
if dropped:
    logging.warning(
        "flows ignored by the mirror (neither M nor X): %s, %d row(s)",
        dropped, data.filter(pl.col("flowCode").is_in(dropped)).height,
    )

mirror = one_side(data, "X", "exp").join(
    one_side(data, "M", "imp"), on=KEYS, how="full", coalesce=True
)

# A country may appear on one side only: we merge the labels.
for role in ("exporter", "importer"):
    sides = [f"{role}Desc_{s}" for s in ("exp", "imp") if f"{role}Desc_{s}" in mirror.columns]
    if sides:
        mirror = mirror.with_columns(
            pl.coalesce([pl.col(c) for c in sides]).alias(f"{role}Desc")
        ).drop(sides)

mirror = mirror.sort(KEYS)

logging.info("mirror: %d directed edge(s), %d columns",
             mirror.height, mirror.width)
for side, label in (("exp", "exporter"), ("imp", "importer")):
    column = f"primaryValue_{side}"
    if column in mirror.columns:
        logging.info("edges with no %s declaration: %d", label,
                     mirror[column].null_count())

# The mirror moves value between columns; it must never lose any. A row
# silently dropped by the group_by or the full join would leave a file that
# opens and sums to something plausible. Checked before writing, so a broken
# mirror is not delivered.
findings = check_mirror(data, mirror)
for finding in findings:
    log = logging.info if finding["status"] == "pass" else logging.warning
    log("check %-12s %-4s %s", finding["check"], finding["status"],
        finding["detail"])
raise_on_fail(findings, "comtrade_mirror")

outputs = {Path(p).suffix.lstrip("."): p for p in snakemake.output}
if "parquet" in outputs:
    mirror.write_parquet(outputs["parquet"], compression="zstd")
if "csv" in outputs:
    mirror.write_csv(outputs["csv"], separator=snakemake.params["csv_sep"])
