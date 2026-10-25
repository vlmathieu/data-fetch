"""Regenerates resources/comtrade_areas.{json,csv} from UNSD's public tables.

Run BY HAND, outside the workflow:

    python workflow/scripts/build_comtrade_areas.py

Not a Snakemake rule on purpose: it writes to resources/, which is versioned,
not to data/. The table is committed rather than fetched at runtime, for three
reasons:

  - resolve_params runs at DAG time. A network call there would make
    `snakemake -n` fail on a connection blip.
  - comtrade_common.py must stay cheap to import: no pandas, no urllib.
  - the fingerprint of a request must not depend on a remote file UNSD can
    rewrite without notice.

Endpoints are PUBLIC: no API key, no quota.
"""

from __future__ import annotations

import csv
import json
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

REPORTERS = "https://comtradeapi.un.org/files/v1/app/reference/Reporters.json"
PARTNERS = "https://comtradeapi.un.org/files/v1/app/reference/partnerAreas.json"

ROOT = Path(__file__).resolve().parents[2]
OUT_JSON = ROOT / "resources" / "comtrade_areas.json"
OUT_CSV = ROOT / "resources" / "comtrade_areas.csv"
EU_CSV = ROOT / "resources" / "eu_membership.csv"

# --- what is not a country --------------------------------------------------
# An explicit list, not a rule. Every mechanical test fails in BOTH directions:
#
#   - "no valid ISO3" misses the European Union (97/EUR), "Europe EU, nes"
#     (492/MCO, which carries Monaco's ISO3) and the Neutral Zone (536/NTZ).
#   - "no valid ISO3" also catches real places that simply have no ISO3:
#     Kosovo, the Channel Islands, Sarawak.
#   - UNSD's own `isGroup` flag marks only 4 of the 312 entities: Bunkers,
#     Free Zones and every "..., nes" are not flagged.
#
# So the judgement is written down here, once, and reviewable in the CSV.
# `kind` drives the exclude_pseudo filter: aggregate and territory are dropped,
# country and proxy are kept.
NOT_A_COUNTRY: dict[int, tuple[str, str]] = {
    # Aggregates and residuals: a sum of several countries, or a catch-all.
    # Keeping them double counts bilateral flows.
      0: ("aggregate", "every partner summed; duplicates all bilateral flows"),
     97: ("aggregate", "aggregate reporter, no partner code, reports from 2000"),
    129: ("aggregate", "residual: Caribbean not elsewhere specified"),
    221: ("aggregate", "residual: Eastern Europe not elsewhere specified"),
    290: ("aggregate", "residual: Northern Africa not elsewhere specified"),
    471: ("aggregate", "residual: Central American Common Market, nes"),
    472: ("aggregate", "residual: Africa CAMEU region, nes"),
    473: ("aggregate", "residual: Latin American Integration Association, nes"),
    492: ("aggregate", "residual despite carrying Monaco's ISO3 (MCO); UNSD "
                       "note reads 'Europe EU, not elsewhere specified'"),
    527: ("aggregate", "residual: Oceania not elsewhere specified"),
    568: ("aggregate", "residual: other Europe not elsewhere specified"),
    577: ("aggregate", "residual: other Africa not elsewhere specified"),
    636: ("aggregate", "residual: rest of America not elsewhere specified"),
    637: ("aggregate", "residual: North and Central America, nes"),
    697: ("aggregate", "residual: EFTA not elsewhere specified"),
    711: ("aggregate", "customs union aggregate, closed 1999"),
    837: ("aggregate", "fuel and stores supplied to ships and aircraft, "
                       "not a destination"),
    838: ("aggregate", "free zones, not a destination"),
    839: ("aggregate", "special categories, not a destination"),
    879: ("aggregate", "residual: Western Asia not elsewhere specified"),
    899: ("aggregate", "catch-all for unallocated areas"),
    975: ("aggregate", "aggregate reporter, no partner code, reports from 2010"),

    # Real places with no ISO3 in UNSD's table. Dropped by exclude_pseudo
    # because they cannot be joined to anything keyed on ISO3, the mirror
    # included, but they are not double counts.
    412: ("territory", "real reporter, no ISO3 assigned by UNSD"),
    457: ("territory", "real place, no ISO3"),
    459: ("territory", "historical, merged into Malaysia in 1963"),
    461: ("territory", "historical, merged into Malaysia in 1963"),
    488: ("territory", "real place, no ISO3"),
    536: ("territory", "Saudi-Iraq neutral zone, dissolved 1991; NTZ is not "
                       "an ISO 3166 code"),
    647: ("territory", "historical, returned to Japan in 1972"),
    698: ("territory", "historical, merged into India in 1975"),
    717: ("territory", "historical, dissolved 1964"),
    830: ("territory", "real place, no ISO3"),
    835: ("territory", "historical, became Tanzania in 1964"),
    836: ("territory", "historical, became Tanzania in 1964"),
    872: ("territory", "real place, no ISO3"),

    # Kept by the filter.
    490: ("proxy", "TAIWAN. UNSD cannot name it, so publishes it under this "
                   "residual label. A single reporting economy, not a sum of "
                   "countries: dropping it would delete Taiwan's trade"),
}

VALID_KINDS = ("country", "proxy", "aggregate", "territory")


def fetch(url: str) -> list[dict]:
    """Downloads one public reference table.

    Parameters
    ----------
    url : str
        Full URL of the JSON reference file.

    Returns
    -------
    list of dict
        The 'results' array of the file.
    """
    with urllib.request.urlopen(url, timeout=120) as response:  # noqa: S310
        return json.loads(response.read())["results"]


def real_iso3(iso3: str) -> bool:
    """Is this a genuine ISO 3166-1 alpha-3 code?

    UNSD fills the field with its own placeholders for everything that has no
    ISO3: 'W00' (World), 'S19', '_X', 'R4', 'X1'. They are all caught by
    requiring exactly three ASCII uppercase letters.

    Parameters
    ----------
    iso3 : str
        Value of the IsoAlpha3 field, possibly empty or padded.

    Returns
    -------
    bool
        True for 'FRA', False for 'W00', '_X', 'R4 ' or ''.
    """
    return len(iso3) == 3 and iso3.isalpha() and iso3.isupper()


def build() -> list[dict]:
    """Merges the reporter and partner tables into one list of areas.

    Returns
    -------
    list of dict
        One entry per Comtrade code, sorted by code, with keys code, iso3,
        desc, kind, note, is_reporter, is_partner.

    Raises
    ------
    ValueError
        If an entity has no valid ISO3 and is absent from NOT_A_COUNTRY. That
        means UNSD added an area since this list was written: classify it by
        hand rather than let it through as a country.
    """
    areas: dict[int, dict] = {}

    for row in fetch(REPORTERS):
        code = int(row["reporterCode"])
        areas[code] = {
            "code": code,
            "iso3": (row.get("reporterCodeIsoAlpha3") or "").strip(),
            "desc": (row.get("reporterDesc") or "").strip(),
            "is_reporter": True,
            "is_partner": False,
        }

    for row in fetch(PARTNERS):
        code = int(row["PartnerCode"])
        entry = areas.setdefault(code, {
            "code": code,
            "iso3": (row.get("PartnerCodeIsoAlpha3") or "").strip(),
            "desc": (row.get("PartnerDesc") or "").strip(),
            "is_reporter": False,
            "is_partner": False,
        })
        entry["is_partner"] = True

    unknown: list[str] = []
    for entry in areas.values():
        kind, note = NOT_A_COUNTRY.get(entry["code"], ("", ""))
        if not kind:
            if real_iso3(entry["iso3"]):
                kind, note = "country", ""
            else:
                unknown.append(
                    f"{entry['code']} ({entry['iso3']!r}) {entry['desc']}"
                )
                continue
        entry["kind"], entry["note"] = kind, note

    if unknown:
        raise ValueError(
            "areas with no valid ISO3 and no entry in NOT_A_COUNTRY:\n  "
            + "\n  ".join(unknown)
            + "\nUNSD has added areas. Classify each one by hand as "
              "'aggregate', 'territory' or 'proxy'."
        )
    return sorted(areas.values(), key=lambda e: e["code"])


def check_eu_members(areas: list[dict]) -> None:
    """Every ISO3 of eu_membership.csv must resolve to at least one area.

    Catches a typo in the membership file here, at build time, rather than as
    an EU aggregation that silently forgets a member.

    Parameters
    ----------
    areas : list of dict
        Output of build().

    Raises
    ------
    ValueError
        An ISO3 of the membership file matches no area.
    """
    if not EU_CSV.exists():
        print(f"  (no {EU_CSV.name} yet, membership check skipped)")
        return
    known = {e["iso3"] for e in areas if e["kind"] == "country"}
    with EU_CSV.open(encoding="utf-8") as handle:
        missing = sorted(
            row["iso3"] for row in csv.DictReader(handle)
            if row["iso3"] not in known
        )
    if missing:
        raise ValueError(f"{EU_CSV.name}: unknown ISO3 {missing}")
    print(f"  {EU_CSV.name}: every ISO3 resolves")


if __name__ == "__main__":
    areas = build()
    check_eu_members(areas)

    counts = {kind: sum(1 for e in areas if e["kind"] == kind)
              for kind in VALID_KINDS}

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps({
        "source": [REPORTERS, PARTNERS],
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "generated_by": "workflow/scripts/build_comtrade_areas.py",
        "kinds": {
            "country": "real country or territory with an ISO 3166-1 alpha-3 code",
            "proxy": "single reporting economy published under a residual label",
            "aggregate": "sum of several areas, or a catch-all; dropped by exclude_pseudo",
            "territory": "real place with no ISO3; dropped by exclude_pseudo",
        },
        "counts_by_kind": counts,
        "areas": areas,
    }, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    with OUT_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["code", "iso3", "desc", "kind", "is_reporter",
                        "is_partner", "note"],
        )
        writer.writeheader()
        writer.writerows(areas)

    print(f"{len(areas)} areas -> {OUT_JSON.relative_to(ROOT)}")
    print(f"{' ' * len(str(len(areas)))} -> {OUT_CSV.relative_to(ROOT)}")
    for kind, n in counts.items():
        print(f"  {kind:<10} {n:>4}")
