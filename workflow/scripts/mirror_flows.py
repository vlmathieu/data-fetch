"""Flux miroirs : les deux déclarations d'une même arête, côte à côte.

Un flux bilatéral est déclaré deux fois — par l'exportateur (flowCode 'X') et
par l'importateur ('M'). Ce script ne les réconcilie PAS : il les met côte à
côte, suffixés `_exp` et `_imp`.

Deux règles, héritées de trade_network_analysis et non négociables :

  - Jamais de réconciliation. Pas de moyenne FOB/CIF, pas de substitution d'un
    côté par l'autre. L'écart entre les deux déclarations est un signal de
    qualité de la donnée, pas du bruit à moyenner.
  - Les nuls restent nuls, pas zéro. Les sommes sont gardées : un groupe
    entièrement nul reste nul. Sinon un non-déclarant devient un déclarant à
    zéro et le diagnostic disparaît.
"""

# Pas de `from __future__ import annotations` : Snakemake insère un
# préambule avant ce fichier, et un import __future__ doit être en
# première ligne. Python 3.12 gère nativement `str | None`.
import logging
import os
import sys
from pathlib import Path

from snakemake.script import snakemake

os.environ["POLARS_MAX_THREADS"] = str(snakemake.threads)
import polars as pl  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))

logging.basicConfig(
    filename=snakemake.log[0],
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Grandeurs mesurées de Comtrade, dans l'ordre de leur apparition en colonnes.
MEASURES = ("netWgt", "grossWgt", "qty", "altQty",
            "fobvalue", "cifvalue", "primaryValue")

# L'exportateur et l'importateur ne sont pas le même acteur selon le sens du
# flux : dans un rapport d'export, le déclarant est l'exportateur.
ROLES = {
    "X": {"reporterISO": "exporterISO", "reporterDesc": "exporterDesc",
          "partnerISO": "importerISO", "partnerDesc": "importerDesc"},
    "M": {"reporterISO": "importerISO", "reporterDesc": "importerDesc",
          "partnerISO": "exporterISO", "partnerDesc": "exporterDesc"},
}

KEYS = ["period", "cmdCode", "exporterISO", "importerISO"]


def guarded_sum(column: str) -> pl.Expr:
    """Somme qui reste nulle si le groupe n'a aucune valeur renseignée.

    `sum()` seul renvoie 0 sur un groupe tout-nul, ce qui transforme un
    non-déclarant en déclarant à zéro.
    """
    return (
        pl.when(pl.col(column).count() > 0)
        .then(pl.col(column).sum())
        .otherwise(None)
        .alias(column)
    )


def one_side(data: pl.DataFrame, flow_code: str, suffix: str) -> pl.DataFrame:
    """Un côté du miroir : les déclarations d'un seul sens de flux."""
    rename = {k: v for k, v in ROLES[flow_code].items() if k in data.columns}
    measures = [m for m in MEASURES if m in data.columns]
    side = data.filter(pl.col("flowCode") == flow_code).rename(rename)

    descriptions = [c for c in ("exporterDesc", "importerDesc") if c in side.columns]
    aggregated = side.group_by(KEYS).agg(
        [guarded_sum(m) for m in measures]
        # Le libellé dépend de l'ISO : premier non-nul, pas une clé de groupe.
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
        "flux ignorés par le miroir (ni M ni X) : %s — %d ligne(s)",
        dropped, data.filter(pl.col("flowCode").is_in(dropped)).height,
    )

mirror = one_side(data, "X", "exp").join(
    one_side(data, "M", "imp"), on=KEYS, how="full", coalesce=True
)

# Un pays peut n'apparaître que d'un seul côté : on réunit les libellés.
for role in ("exporter", "importer"):
    sides = [f"{role}Desc_{s}" for s in ("exp", "imp") if f"{role}Desc_{s}" in mirror.columns]
    if sides:
        mirror = mirror.with_columns(
            pl.coalesce([pl.col(c) for c in sides]).alias(f"{role}Desc")
        ).drop(sides)

mirror = mirror.sort(KEYS)

logging.info("miroir : %d arête(s) dirigée(s), %d colonnes",
             mirror.height, mirror.width)
for side, label in (("exp", "exportateur"), ("imp", "importateur")):
    column = f"primaryValue_{side}"
    if column in mirror.columns:
        logging.info("arêtes sans déclaration %s : %d", label,
                     mirror[column].null_count())

outputs = {Path(p).suffix.lstrip("."): p for p in snakemake.output}
if "parquet" in outputs:
    mirror.write_parquet(outputs["parquet"], compression="zstd")
if "csv" in outputs:
    mirror.write_csv(outputs["csv"], separator=snakemake.params["csv_sep"])
