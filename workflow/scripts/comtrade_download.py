"""Tirage selon le plan du pré-vol, puis écriture des données et du sidecar.

Seule étape qui consomme du quota premium. La clé API est lue ICI, dans le
script, et jamais passée en `params:` d'une règle : Snakemake enregistre tous
les params en clair dans .snakemake/metadata/, ce qui laisse le secret sur le
disque.
"""

# Pas de `from __future__ import annotations` : Snakemake insère un
# préambule avant ce fichier, et un import __future__ doit être en
# première ligne. Python 3.12 gère nativement `str | None`.
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
from snakemake.script import snakemake

sys.path.insert(0, str(Path(__file__).resolve().parent))
from comtrade_common import (  # noqa: E402
    FAIL_ROWS,
    WARN_ROWS,
    call,
    params_hash,
    selection,
)

logging.basicConfig(
    filename=snakemake.log[0],
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

APIKEY = os.environ["comtrade_apikey"]


def fetch_pack(params: dict, pack: dict) -> pl.DataFrame:
    """Un appel premium par paquet, avec contrôle du plafond.

    getFinalData plafonne à 250 000 enregistrements et tronque SANS erreur au
    plafond. On échoue plutôt que de livrer un fichier amputé en silence.
    """
    fn = (comtradeapicall.getFinalData if params["data_type"] == "final"
          else comtradeapicall.getTarifflineData)
    period = ",".join(pack["periods"])
    label = f"{period} / {len(pack['cmd'] or params['cmd'])} code(s)"

    frame = call(
        fn, APIKEY, label=label, includeDesc=params["include_desc"],
        format_output="JSON", **selection(params, period, pack["cmd"]),
    )
    rows = len(frame)

    if rows >= FAIL_ROWS:
        raise RuntimeError(
            f"[{label}] {rows} lignes ≥ plafond {FAIL_ROWS} de getFinalData : "
            f"la réponse est tronquée. Restreindre les produits ou la période."
        )
    if rows >= WARN_ROWS:
        logging.warning("[%s] %d lignes ≥ seuil d'alerte %d", label, rows, WARN_ROWS)

    logging.info("[%s] %d lignes (attendues : %d)", label, rows,
                 pack["expected_rows"])
    return pl.from_pandas(frame) if rows else pl.DataFrame()


def _pkg_version(name: str) -> str:
    """Version installée : une sortie qui change sans que le code change."""
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return "inconnu"


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
        warnings.append(f"paquet {','.join(pack['periods'])} : 0 ligne reçue")

if not frames:
    raise RuntimeError("aucune donnée reçue pour cette sélection")

data = pl.concat(frames, how="vertical_relaxed")

# Contrôle d'intégrité : aucune période demandée ne doit manquer en silence.
got = set(data["period"].cast(pl.String).unique())
missing = sorted(set(params["period"]) - got)
if missing:
    warnings.append(f"périodes demandées absentes du résultat : {missing}")
    logging.warning("périodes absentes : %s", missing)

logging.info("total : %d lignes, %d colonnes", data.height, data.width)

# --- écritures --------------------------------------------------------------

outputs = {Path(p).suffix.lstrip("."): p for p in snakemake.output}
meta_path = next(p for p in snakemake.output if p.endswith(".meta.json"))
base = meta_path[: -len(".meta.json")]  # radical commun, sidecar compris

if "parquet" in outputs:
    data.write_parquet(outputs["parquet"], compression="zstd")
if "csv" in outputs:
    # polars quote automatiquement les champs contenant le séparateur, donc
    # « China, Hong Kong SAR » survit à un séparateur virgule. Les zéros
    # initiaux, eux, sont perdus à la RELECTURE du CSV : le pipeline ne lit
    # jamais ce fichier, le parquet est l'artefact canonique.
    data.write_csv(outputs["csv"], separator=snakemake.params["csv_sep"])

if not params["include_desc"]:
    # Sans les libellés dans les données, les tables de référence doivent être
    # livrées à côté, sinon le fichier n'est pas interprétable.
    for category in ("reporter", "partner", f"cmd:{params['classification']}"):
        try:
            ref = call(comtradeapicall.getReference, category, attempts=2,
                       label=f"reference {category}")
        except RuntimeError as exc:
            warnings.append(f"table de référence {category} indisponible : {exc}")
            continue
        name = category.replace(":", "_")
        pl.from_pandas(ref).write_csv(
            f"{base}.ref_{name}.csv", separator=snakemake.params["csv_sep"]
        )

# Lien par hash, pas un `latest` global : un latest unique devient faux dès
# qu'il y a deux requêtes différentes dans le même dossier.
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
# Jamais la clé API dans le sidecar.
Path(meta_path).write_text(
    json.dumps(sidecar, indent=2, ensure_ascii=False), encoding="utf-8"
)
logging.info("sidecar écrit : %s", meta_path)
