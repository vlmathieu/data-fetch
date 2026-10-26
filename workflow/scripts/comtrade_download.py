"""Downloads according to the preflight plan, then writes the data and sidecar.

The only step that spends premium quota. The API key is read HERE, in the
script, and never passed in a rule's `params:`: Snakemake records every param
in cleartext in .snakemake/metadata/, which leaves the secret on disk.
"""

# No `from __future__ import annotations` here: Snakemake inserts a preamble
# before this file, and a __future__ import has to be on the first line.
# Python 3.12 handles `str | None` natively.
import json
import logging
import os
import subprocess
import sys
from importlib import metadata
from datetime import datetime, timezone
from pathlib import Path

import comtradeapicall
import polars as pl
# `snakemake` is NOT imported: the preamble Snakemake inserts above this file
# defines it as a global. The old `from snakemake.script import snakemake` only
# worked because Snakemake 8's own preamble also set it as a module attribute;
# Snakemake 9 dropped that, so the import raises ImportError there. Relying on
# the injected global works on both. Hence the noqa: F821 on its first uses --
# a linter cannot see the preamble.

sys.path.insert(0, str(Path(__file__).resolve().parent))
from comtrade_common import (  # noqa: E402
    call,
    caps,
    compare_vintage,
    params_hash,
    pseudo_codes,
    raise_on_fail,
    revision_finding,
    selection,
    vintage,
)
from comtrade_polars import (  # noqa: E402
    check_csv,
    check_download,
    drop_pseudo,
)

logging.basicConfig(
    filename=snakemake.log[0],  # noqa: F821
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

try:
    APIKEY = os.environ["comtrade_apikey"]
except KeyError:
    # A bare KeyError says nothing about what to do. No `envvars:` directive
    # guards this upstream, on purpose: it would make the WHOLE workflow refuse
    # to start without a key, preflight included, and preflight needs none.
    raise RuntimeError(
        "comtrade_apikey is not set in the environment.\n"
        "\n"
        "comtrade_download is the ONLY rule that needs it. preflight, mirror "
        "and eu run without, so nothing before this point was wasted: the "
        "plan.json is on disk and the counting calls were free.\n"
        "\n"
        "  export comtrade_apikey='...'      # this shell only\n"
        "\n"
        "To keep it across sessions, put that line in ~/.zshrc, not in a file "
        "of your own that nothing sources. Beware: ~/.zshrc is read only by "
        "INTERACTIVE shells. A script, a CI job or an agent runs a "
        "non-interactive shell, which reads ~/.zshenv instead.\n"
        "\n"
        "Free key: https://comtradedeveloper.un.org/ (see README, 'API key')."
    ) from None


def fetch_pack(params: dict, pack: dict) -> pl.DataFrame:
    """One API call per pack, with a check against the ceiling.

    getFinalData truncates WITHOUT error at the ceiling of your subscription
    tier (params["records_per_call"]). We fail rather than silently deliver an
    amputated file.
    """
    warn_rows, fail_rows = caps(params)
    fn = (comtradeapicall.getFinalData if params["data_type"] == "final"
          else comtradeapicall.getTarifflineData)
    period = ",".join(pack["periods"])
    label = f"{period} / {len(pack['cmd'] or params['cmd'])} code(s)"

    frame = call(
        fn, APIKEY, label=label, includeDesc=params["include_desc"],
        format_output="JSON", **selection(params, period, pack["cmd"]),
    )
    rows = len(frame)

    if rows == 0 and pack["expected_rows"] > 0:
        # The free preflight counted rows here and the keyed call returned
        # none. That is not missing data, it is something broken: a silently
        # swallowed HTTP error, or a selection that did not reach the API as
        # written. It used to be a warning buried in the sidecar.
        raise RuntimeError(
            f"[{label}] 0 rows received, {pack['expected_rows']} counted by "
            f"the preflight. The request went out but came back empty: check "
            f"the log above for an API error body."
        )

    if rows >= fail_rows:
        raise RuntimeError(
            f"[{label}] {rows} rows >= the {fail_rows} getFinalData ceiling: "
            f"the response is truncated. Narrow the products or the period, or "
            f"raise records_per_call if your key allows it."
        )
    if rows >= warn_rows:
        logging.warning("[%s] %d rows >= warning threshold %d", label, rows, warn_rows)

    logging.info("[%s] %d rows (expected: %d)", label, rows,
                 pack["expected_rows"])
    return pl.from_pandas(frame) if rows else pl.DataFrame()


def _pkg_version(name: str) -> str:
    """Installed version: an output that changes without the code changing."""
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return "unknown"


def git_commit() -> str | None:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True,
            check=True, cwd=Path(__file__).resolve().parent,
        ).stdout.strip()
    except (subprocess.CalledProcessError, OSError):
        return None


plan = json.loads(Path(snakemake.input[0]).read_text(encoding="utf-8"))
params = plan["params_resolved"]

frames, warnings = [], list(plan["warnings"])
for pack in plan["packs"]:
    frame = fetch_pack(params, pack)
    if frame.height:
        frames.append(frame)
    else:
        # Only reachable when the preflight itself counted 0 for every period
        # of the pack, which it has already warned about. An unexpected empty
        # pack raises inside fetch_pack instead.
        warnings.append(f"pack {','.join(pack['periods'])}: 0 rows received")

if not frames:
    raise RuntimeError("no data received for this selection")

data = pl.concat(frames, how="vertical_relaxed")
rows_received = data.height

# Checks BEFORE the filter and before any write, and the order is not a
# detail. The preflight counts are pre-filter by design, so comparing them
# after exclude_pseudo reports a deficit on every period of a healthy run:
# measured at -326 to -328 per period on 2026-10-26. And a failing check must
# leave no file behind: Snakemake deletes the `output:` of a failed job but
# keeps its `log:`, so the diagnosis below survives the failure while the
# non-conforming dataset does not.
#
# The logic is in comtrade_polars, where it is testable: a `script:` file like
# this one cannot be imported, so a check written here could never be shown a
# broken dataset to prove it detects anything.
findings = check_download(data, plan)
for finding in findings:
    log = logging.info if finding["status"] == "pass" else logging.warning
    log("check %-16s %-4s %s", finding["check"], finding["status"],
        finding["detail"])
# Anything not passing also goes in `warnings`, so that what already read that
# field loses nothing to the move.

# A count gap is ambiguous on its own: Comtrade revises upstream as often as we
# get something wrong. The checksum tells them apart, so it is re-read HERE --
# and only when there is a gap to explain. Free, public calls; a healthy run
# spends none of them, because silence is cheaper still.
gap = any(f["check"].startswith("counts_") and f["status"] != "pass"
          for f in findings)
comparison = None
if gap:
    logging.info("counts differ: re-reading the vintage to tell a Comtrade "
                 "revision from a bug here")
    try:
        after, status = vintage(params, params["period"], comtradeapicall)
        comparison = (compare_vintage(plan["vintage"], after)
                      if status == "complete" else None)
    except RuntimeError as exc:
        logging.warning("vintage could not be re-read: %s", exc)
verdict = revision_finding(gap, comparison)
findings.append(verdict)
# Logged whether or not it fired, like every other check: a log showing one
# fewer line than the sidecar's `checks` block is a puzzle nobody needs.
log = logging.info if verdict["status"] == "pass" else logging.warning
log("check %-16s %-4s %s", verdict["check"], verdict["status"],
    verdict["detail"])

warnings += [f"{f['check']}: {f['detail']}"
             for f in findings if f["status"] != "pass"]
raise_on_fail(findings, "comtrade_download")

dropped_pseudo: dict[str, int] = {}

if params["exclude_pseudo"]:
    # Drop what is not a country: World (which duplicates every bilateral
    # flow), the European Union and ASEAN aggregate reporters, the "..., nes"
    # residuals, Bunkers, Free Zones, and the territories with no ISO3.
    # Code 490 is KEPT: it is Taiwan, a single reporting economy.
    #
    # The logic is in comtrade_polars.drop_pseudo, which has a self-check: a
    # `script:` file like this one cannot be imported, so nothing here is
    # testable. That is not theoretical — the first version of this filter
    # looked at partner2Code and emptied the whole dataset.
    data, pseudo_report = drop_pseudo(data, pseudo_codes())
    dropped_pseudo = pseudo_report["dropped"]

    logging.info("exclude_pseudo: %d rows -> %d (%d dropped)",
                 rows_received, data.height, rows_received - data.height)
    for key, n in sorted(dropped_pseudo.items(), key=lambda kv: -kv[1]):
        logging.info("  dropped %s: %d row(s)", key, n)

    if not data.height:
        raise RuntimeError(
            "exclude_pseudo removed every row: this selection contains only "
            "aggregates and areas with no ISO3. Set exclude_pseudo: false if "
            "that is what you want."
        )

logging.info("total: %d rows, %d columns", data.height, data.width)

# --- writes -----------------------------------------------------------------

outputs = {Path(p).suffix.lstrip("."): p for p in snakemake.output}
meta_path = next(p for p in snakemake.output if p.endswith(".meta.json"))
base = meta_path[: -len(".meta.json")]  # shared stem, sidecar included

if "parquet" in outputs:
    data.write_parquet(outputs["parquet"], compression="zstd")
if "csv" in outputs:
    # polars quotes fields containing the separator automatically, so
    # "China, Hong Kong SAR" survives a comma separator. Leading zeros, on the
    # other hand, are lost when the CSV is READ BACK: the pipeline never reads
    # this file, the parquet is the canonical artifact.
    data.write_csv(outputs["csv"], separator=snakemake.params["csv_sep"])
    csv_findings = check_csv(outputs["csv"], data, snakemake.params["csv_sep"])
    for f in csv_findings:
        log = logging.info if f["status"] == "pass" else logging.warning
        log("check %-16s %-4s %s", f["check"], f["status"], f["detail"])
    findings += csv_findings
    raise_on_fail(csv_findings, "comtrade_download")

if not params["include_desc"]:
    # Without the labels in the data, the reference tables have to be delivered
    # alongside, otherwise the file cannot be interpreted.
    for category in ("reporter", "partner", f"cmd:{params['classification']}"):
        try:
            ref = call(comtradeapicall.getReference, category, attempts=2,
                       label=f"reference {category}")
        except RuntimeError as exc:
            warnings.append(f"reference table {category} unavailable: {exc}")
            continue
        name = category.replace(":", "_")
        pl.from_pandas(ref).write_csv(
            f"{base}.ref_{name}.csv", separator=snakemake.params["csv_sep"]
        )

# Link by hash, not a global `latest`: a single latest becomes wrong as soon as
# there are two different requests in the same folder.
for extension in ("parquet", "csv"):
    if extension in outputs:
        target = Path(outputs[extension])
        link = target.parent / f"latest_{params_hash(params)}.{extension}"
        link.unlink(missing_ok=True)
        link.symlink_to(target.name)

sidecar = {
    "params_resolved": params,
    "params_hash": params_hash(params),
    "run_id": plan["run_id"],
    "requested_at": plan["created_at"],
    "written_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "rows": data.height,
    # Rows as received, before exclude_pseudo. The plan's expected_rows_total
    # is an upper bound of `rows` by design, but it should match this one.
    "rows_received": rows_received,
    "rows_dropped_pseudo": rows_received - data.height,
    "excluded_codes": dropped_pseudo,
    # One entry per check, so a run is auditable without reading the log.
    # Only written on a successful run: a failing check raises before this
    # point, and the log is then the record.
    "checks": {f["check"]: {k: v for k, v in f.items() if k != "check"}
               for f in findings},
    "columns": data.columns,
    "api_calls": {"preflight_free": plan["preflight_calls"],
                  "download_premium": len(plan["packs"])},
    "plan": plan["packs"],
    "thresholds": plan["thresholds"],
    "vintage_status": plan["vintage_status"],
    "vintage": plan["vintage"],
    "warnings": warnings,
    "versions": {"comtradeapicall": _pkg_version("comtradeapicall"),
                 "polars": pl.__version__},
    "git_commit": git_commit(),
}
# Never the API key in the sidecar.
Path(meta_path).write_text(
    json.dumps(sidecar, indent=2, ensure_ascii=False), encoding="utf-8"
)
logging.info("sidecar written: %s", meta_path)
