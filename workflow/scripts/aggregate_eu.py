"""Rebuilds the European Union as one node, by summing its member states.

NOT by selecting Comtrade's own code 97. That code is unusable for bilateral
work, and measurably so:

  - it exists ONLY as a reporter. There is no 'European Union' partner code, so
    a selection on 97 would give the EU's own declarations and nothing declared
    towards it.
  - it reports nothing before 2000 (measured on the wood codes), so a
    1996-2023 request would carry four silently empty years.

Summing the members has neither problem: it works on rows we already hold, on
both sides at once, over the whole period.

A DERIVED file, like the mirror: the canonical parquet is left untouched, so
the aggregation can be redone without downloading anything again.

The logic lives in comtrade_polars.aggregate_eu, which has a runnable check
(`python workflow/scripts/comtrade_polars.py`). This file is only the wiring:
a `script:` file cannot be run on its own.
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
from comtrade_polars import aggregate_eu, eu_membership  # noqa: E402

logging.basicConfig(
    filename=snakemake.log[0],
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

params = snakemake.params["params"]
if not params["include_desc"]:
    # Explicit refusal rather than a ColumnNotFoundError twenty lines later:
    # the aggregation keys on the ISO columns, which include_desc: false does
    # not deliver.
    raise RuntimeError(
        "aggregate_eu needs the ISO columns, which include_desc: false does "
        "not deliver. Set include_desc: true, or aggregate_eu: false."
    )

members = eu_membership()
logging.info("membership: %d Comtrade code(s) over %d member state(s)",
             members.height, members["code"].n_unique())

data = pl.read_parquet(snakemake.input[0])
logging.info("input: %d rows, %d columns", data.height, data.width)

result, report = aggregate_eu(data, members)

logging.info("intra-EU rows deleted: %d", report["intra_eu_dropped"])
logging.info("rows: %d -> %d, of which %d touch the EU node",
             report["rows_in"], report["rows_out"],
             report["rows_touching_eu"])
# A reduced view, and the reduction is named: these columns described a single
# declaration and mean nothing once several are summed.
logging.info("columns dropped (per-declaration): %s",
             report["columns_dropped"])

if not result.height:
    raise RuntimeError("EU aggregation produced no rows")
if not report["rows_touching_eu"]:
    logging.warning(
        "no row touches the EU node: no member state in this selection, or "
        "none within its membership years"
    )

outputs = {Path(p).suffix.lstrip("."): p for p in snakemake.output}
if "parquet" in outputs:
    result.write_parquet(outputs["parquet"], compression="zstd")
if "csv" in outputs:
    result.write_csv(outputs["csv"], separator=snakemake.params["csv_sep"])
logging.info("written: %s", list(outputs.values()))
