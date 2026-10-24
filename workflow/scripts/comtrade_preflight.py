"""Pré-vol : compte les lignes et relève le millésime, puis écrit le plan de tirage.

Tout ce que fait ce script passe par les endpoints PUBLICS de Comtrade, sans
clé et hors quota premium :

  - previewFinalData(..., countOnly=True) renvoie le compte RÉEL, non plafonné.
    Le « limited to 500 records » du README ne porte que sur les lignes de
    données, pas sur le scalaire. Vérifié : France 2015 tous produits → 674 847,
    identique au totalRecords de l'endpoint availability.
  - _getFinalDataAvailability donne classification, totalRecords, checksum et
    date de dernière publication par (reporter, période).

Conséquence : planifier le découpage à l'optimum ne coûte rien. Seul le tirage
consomme du quota.

Limite de l'endpoint public : une seule période par appel
(`{"error":"Maximum number of periods for preview is 1"}`), d'où un appel par
période. Sans importance puisque c'est gratuit.
"""

# Pas de `from __future__ import annotations` : Snakemake insère un
# préambule avant ce fichier, et un import __future__ doit être en
# première ligne. Python 3.12 gère nativement `str | None`.
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import comtradeapicall
from snakemake.script import snakemake

sys.path.insert(0, str(Path(__file__).resolve().parent))
from comtrade_common import (  # noqa: E402
    FAIL_ROWS,
    FIXED,
    WARN_ROWS,
    bisect_codes,
    call,
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
    """Compte gratuit, une période, via l'endpoint public."""
    fn = (comtradeapicall.previewFinalData if params["data_type"] == "final"
          else comtradeapicall.previewTarifflineData)
    df = call(fn, label=f"count {period}", countOnly=True,
              **selection(params, period, cmd))
    return int(df["count"].iloc[0])


def vintage(params: dict, periods: list[str], chunk: int = 10
            ) -> tuple[list[dict], str]:
    """Millésime par (reporter, période) : quelle version des données on tire.

    Comtrade réécrit l'historique — France 2020 a été publié le 2021-05-26 puis
    révisé le 2026-05-23. Sans ce relevé, deux tirages du même paramétrage qui
    divergent sont inexplicables. `datasetChecksum` est l'ancre la plus forte :
    inchangé = données identiques.
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
            logging.warning("millésime indisponible pour %s : %s", batch, exc)
            return rows, "incomplet"
        present = [c for c in keep if c in df.columns]
        rows.extend(df[present].astype(str).to_dict("records"))
    return rows, "complet"


def build_plan(params: dict) -> dict:
    periods = params["period"]
    warnings: list[str] = []
    calls = 0

    counts: dict[str, int] = {}
    for period in periods:
        counts[period] = count_rows(params, period)
        calls += 1
        logging.info("compte %s : %d lignes", period, counts[period])
        if counts[period] == 0:
            warnings.append(
                f"période {period} : 0 ligne pour les codes demandés. "
                f"En classification {params['classification']}, un code peut ne pas "
                f"exister cette année-là (politique hs_revision: "
                f"{params['hs_revision']})."
            )

    packs: list[dict] = []
    for pack in pack_periods(counts, WARN_ROWS):
        expected = sum(counts[p] for p in pack)
        if len(pack) == 1 and expected >= WARN_ROWS:
            # Chemin d'exception : une seule période dépasse déjà la marge.
            # On scinde sur les codes, toujours avec le comptage gratuit.
            period = pack[0]
            warnings.append(
                f"période {period} : {expected} lignes ≥ {WARN_ROWS}, "
                f"scindée sur les codes."
            )
            for codes, n in bisect_codes(
                params["cmd"], lambda cs: count_rows(params, period, cs), WARN_ROWS
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
        "thresholds": {"warn_rows": WARN_ROWS, "fail_rows": FAIL_ROWS},
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
    "plan : %d période(s), %d ligne(s) attendues, %d appel(s) de tirage "
    "(%d appels de comptage, gratuits)",
    len(plan["counts"]), plan["expected_rows_total"], len(plan["packs"]),
    plan["preflight_calls"],
)
for w in plan["warnings"]:
    logging.warning(w)

Path(snakemake.output[0]).write_text(
    json.dumps(plan, indent=2, ensure_ascii=False), encoding="utf-8"
)
