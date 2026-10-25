"""Polars helpers shared by the rule scripts, plus the EU aggregation.

Separate from comtrade_common.py ON PURPOSE: that module is imported by the
Snakefile before any rule is declared, so it must stay free of polars. This one
is imported by scripts only, inside the conda env, so polars is allowed.

Not a `script:` file but an imported module, so `from __future__ import
annotations` is fine here, and so is a runnable self-check:

    python workflow/scripts/comtrade_polars.py
"""

from __future__ import annotations

import csv
from pathlib import Path

import polars as pl

RESOURCES = Path(__file__).resolve().parents[2] / "resources"
MEMBERSHIP_PATH = RESOURCES / "eu_membership.csv"

# Comtrade's measured quantities, in the order their columns appear. Same list
# as the mirror's.
MEASURES = ("netWgt", "grossWgt", "qty", "altQty",
            "fobvalue", "cifvalue", "primaryValue")

EU_ISO = "EU"
EU_DESC = "European Union"

# What identifies one aggregated row. Follows the mirror's precedent of an
# explicit key list rather than "every column that is not a measure": the
# per-declaration columns (isReported, legacyEstimationFlag, isQtyEstimated...)
# differ between member states, so grouping on them would never merge the
# members into one EU node.
#
# qtyUnitCode and altQtyUnitCode ARE keys, deliberately: summing quantities
# declared in different units would be meaningless. Members using different
# units stay on separate rows instead of being silently added up.
EU_KEYS = ("typeCode", "freqCode", "period", "flowCode", "classificationCode",
           "cmdCode", "reporterISO", "partnerISO", "partner2ISO",
           "qtyUnitCode", "altQtyUnitCode")

# Labels: carried over, but they depend on a key rather than being one.
EU_LABELS = ("reporterDesc", "partnerDesc", "partner2Desc", "flowDesc",
             "cmdDesc", "qtyUnitAbbr", "altQtyUnitAbbr")


def guarded_sum(column: str) -> pl.Expr:
    """Sum that stays null if the group has no value filled in.

    `sum()` alone returns 0 on an all-null group, which turns a non-reporter
    into a reporter declaring zero. The gap between two declarations is a data
    quality signal, not noise to average away.

    Parameters
    ----------
    column : str
        Name of the measure column to sum ('netWgt', 'primaryValue'...).

    Returns
    -------
    polars.Expr
        Expression to use inside an `agg`: the sum when the group holds at
        least one non-null value, null otherwise.
    """
    return (
        # count() counts non-null values, unlike len(): "> 0" means "at least
        # one value was actually declared".
        pl.when(pl.col(column).count() > 0)
        .then(pl.col(column).sum())
        .otherwise(None)
        .alias(column)
    )


# --- EU aggregation ---------------------------------------------------------

def eu_membership(resolve_areas=None) -> pl.DataFrame:
    """Dated EU membership, expanded from ISO3 to Comtrade codes.

    resources/eu_membership.csv is keyed on ISO3 so that nothing is typed by
    hand: each member expands to every code it ever had, which is how Belgium
    picks up both code 56 and code 58 (Belgium-Luxembourg, the 1996-1998
    declarant).

    Parameters
    ----------
    resolve_areas : callable, optional
        comtrade_common.resolve_areas. Injected so the self-check can run
        without the areas table; imported on demand when omitted.

    Returns
    -------
    polars dataframe
        Columns 'code' (str), 'from' (int32), 'to' (int32): one row per
        (Comtrade code, membership window). Years are INCLUSIVE, and 9999
        means "still a member".

    Raises
    ------
    FileNotFoundError
        resources/eu_membership.csv is missing.
    ValueError
        An ISO3 in the file matches no area (raised by resolve_areas).
    """
    if resolve_areas is None:
        from comtrade_common import resolve_areas  # noqa: PLC0415

    with MEMBERSHIP_PATH.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    expanded = [
        {"code": code, "from": int(row["from"]), "to": int(row["to"])}
        for row in rows
        for code in resolve_areas([row["iso3"]], "eu_membership")
    ]
    return pl.DataFrame(
        expanded,
        schema={"code": pl.String, "from": pl.Int32, "to": pl.Int32},
    )


def _relabel(data: pl.DataFrame, members: pl.DataFrame, side: str
             ) -> pl.DataFrame:
    """Rewrites one side's ISO and label to EU, for member-years only.

    Parameters
    ----------
    data : polars dataframe
        Trade rows, holding '<side>Code', '<side>ISO', '<side>Desc' and the
        numeric 'year' column added by aggregate_eu.
    members : polars dataframe
        Output of eu_membership().
    side : str
        'reporter' or 'partner'.

    Returns
    -------
    polars dataframe
        The same rows, with that side's ISO set to 'EU' and its label to
        'European Union' wherever the code was a member THAT year. A boolean
        helper column '<side>_is_eu' is left in place for the intra-EU filter.
    """
    flag = f"{side}_is_eu"
    return (
        # Join on the code, then keep the match only if the year falls inside
        # the membership window: a country that joined in 2004 is not an EU row
        # in 2003, and the United Kingdom is not one in 2021.
        data.join(members, left_on=f"{side}Code", right_on="code", how="left")
        .with_columns(
            (pl.col("from").is_not_null()
             & (pl.col("year") >= pl.col("from"))
             & (pl.col("year") <= pl.col("to"))).alias(flag)
        )
        .drop("from", "to", "code", strict=False)
        .with_columns(
            pl.when(pl.col(flag)).then(pl.lit(EU_ISO))
            .otherwise(pl.col(f"{side}ISO")).alias(f"{side}ISO"),
            pl.when(pl.col(flag)).then(pl.lit(EU_DESC))
            .otherwise(pl.col(f"{side}Desc")).alias(f"{side}Desc"),
        )
    )


def aggregate_eu(data: pl.DataFrame, members: pl.DataFrame
                 ) -> tuple[pl.DataFrame, dict]:
    """Folds EU member states into one EU node and deletes intra-EU trade.

    Does NOT use Comtrade's own code 97: that code exists only as a reporter
    (there is no 'European Union' partner code) and reports nothing before
    2000. Summing the members works on both sides and over the whole period.

    Parameters
    ----------
    data : polars dataframe
        Trade rows as written by comtrade_download, with the ISO columns, so
        include_desc must have been true.
    members : polars dataframe
        Output of eu_membership().

    Returns
    -------
    (result, report) : tuple
        result : polars dataframe
            Rows with both sides relabelled, intra-EU flows removed, and the
            measures summed with guarded_sum over EU_KEYS. A REDUCED view: the
            per-declaration columns are dropped, because they describe a single
            declaration and mean nothing once several are summed.
        report : dict
            'intra_eu_dropped', 'columns_dropped', 'rows_in', 'rows_out',
            'rows_touching_eu'. Logged by the caller, so nothing disappears
            silently.
    """
    rows_in = data.height

    # The membership window is in years; a monthly period is YYYYMM.
    data = data.with_columns(
        pl.col("period").cast(pl.String).str.slice(0, 4).cast(pl.Int32)
        .alias("year")
    )
    data = _relabel(data, members, "reporter")
    data = _relabel(data, members, "partner")

    # Intra-EU trade: both ends are now the same node, so the flow is internal
    # to it. Summing it would make the aggregate trade with itself.
    both = pl.col("reporter_is_eu") & pl.col("partner_is_eu")
    intra = data.filter(both).height
    data = data.filter(~both).drop("reporter_is_eu", "partner_is_eu", "year")

    keys = [c for c in EU_KEYS if c in data.columns]
    labels = [c for c in EU_LABELS if c in data.columns]
    measures = [c for c in MEASURES if c in data.columns]
    # Everything else described one declaration, not an aggregate of several:
    # the codes of the member, the estimation flags, the customs and transport
    # modes, refPeriodId. Named in the report rather than quietly removed.
    dropped = [c for c in data.columns
               if c not in (*keys, *labels, *measures)]

    result = (
        data.group_by(keys)
        .agg([guarded_sum(m) for m in measures]
             # A label follows its key, so first non-null rather than a key.
             + [pl.col(c).drop_nulls().first().alias(c) for c in labels])
        .sort(keys)
    )
    return result, {
        "rows_in": rows_in,
        "rows_out": result.height,
        "intra_eu_dropped": intra,
        "columns_dropped": dropped,
        "rows_touching_eu": result.filter(
            (pl.col("reporterISO") == EU_ISO)
            | (pl.col("partnerISO") == EU_ISO)
        ).height,
    }


# --- check ------------------------------------------------------------------

def _demo() -> None:
    """Runnable check of the EU aggregation: python comtrade_polars.py

    Built on a dataframe written by hand, so it needs neither the API nor a
    downloaded file. The repo has no test suite: this function is the check.

    Returns
    -------
    None
        Prints 'comtrade_polars: checks OK', or raises.
    """
    # Dated membership, independent of the areas table so the check stands on
    # its own: Germany and Belgium (two codes) always, the UK until 2020,
    # Croatia from 2013.
    members = pl.DataFrame(
        [{"code": "276", "from": 1958, "to": 9999},   # DEU
         {"code": "56", "from": 1958, "to": 9999},    # BEL
         {"code": "58", "from": 1958, "to": 9999},    # BEL-LUX, 1996-1998
         {"code": "826", "from": 1973, "to": 2020},   # GBR, out after Brexit
         {"code": "191", "from": 2013, "to": 9999}],  # HRV, in from 2013
        schema={"code": pl.String, "from": pl.Int32, "to": pl.Int32},
    )

    def frame(rows: list[dict]) -> pl.DataFrame:
        """One trade row per dict, with the columns the aggregation reads."""
        base = {"typeCode": "C", "freqCode": "A", "flowCode": "X",
                "classificationCode": "H5", "cmdCode": "4403",
                "qtyUnitCode": 8, "altQtyUnitCode": None,
                "reporterDesc": "x", "partnerDesc": "y",
                "isReported": True, "netWgt": None, "primaryValue": None}
        return pl.DataFrame([{**base, **row} for row in rows],
                            infer_schema_length=None)

    # Two members declaring towards the same non-member: one EU row.
    out, report = aggregate_eu(frame([
        {"period": "2015", "reporterCode": "276", "reporterISO": "DEU",
         "partnerCode": "842", "partnerISO": "USA", "primaryValue": 100.0},
        {"period": "2015", "reporterCode": "56", "reporterISO": "BEL",
         "partnerCode": "842", "partnerISO": "USA", "primaryValue": 30.0},
    ]), members)
    assert out.height == 1, out
    assert out["reporterISO"][0] == EU_ISO
    assert out["reporterDesc"][0] == EU_DESC
    assert out["primaryValue"][0] == 130.0, out["primaryValue"][0]
    assert report["intra_eu_dropped"] == 0

    # Intra-EU trade is deleted, not summed into the node.
    out, report = aggregate_eu(frame([
        {"period": "2015", "reporterCode": "276", "reporterISO": "DEU",
         "partnerCode": "56", "partnerISO": "BEL", "primaryValue": 50.0},
        {"period": "2015", "reporterCode": "276", "reporterISO": "DEU",
         "partnerCode": "842", "partnerISO": "USA", "primaryValue": 100.0},
    ]), members)
    assert report["intra_eu_dropped"] == 1, report
    assert out.height == 1 and out["primaryValue"][0] == 100.0, out

    # Dated membership, in both directions.
    out, _ = aggregate_eu(frame([
        {"period": "2019", "reporterCode": "826", "reporterISO": "GBR",
         "partnerCode": "842", "partnerISO": "USA", "primaryValue": 1.0},
        {"period": "2021", "reporterCode": "826", "reporterISO": "GBR",
         "partnerCode": "842", "partnerISO": "USA", "primaryValue": 2.0},
        {"period": "2012", "reporterCode": "191", "reporterISO": "HRV",
         "partnerCode": "842", "partnerISO": "USA", "primaryValue": 3.0},
        {"period": "2013", "reporterCode": "191", "reporterISO": "HRV",
         "partnerCode": "842", "partnerISO": "USA", "primaryValue": 4.0},
    ]), members)
    got = {(r["period"], r["reporterISO"]) for r in out.to_dicts()}
    assert ("2019", EU_ISO) in got, "GBR was an EU member in 2019"
    assert ("2021", "GBR") in got, "GBR left the EU before 2021"
    assert ("2012", "HRV") in got, "HRV joined in 2013, not before"
    assert ("2013", EU_ISO) in got, "HRV was a member in 2013"

    # The historical code of a member is folded in too: without code 58,
    # Belgium 1996-1998 would stay outside the EU node.
    out, _ = aggregate_eu(frame([
        {"period": "1996", "reporterCode": "58", "reporterISO": "BEL",
         "partnerCode": "842", "partnerISO": "USA", "primaryValue": 7.0},
    ]), members)
    assert out["reporterISO"][0] == EU_ISO, out

    # Nulls stay null: an all-null group must not become a reporter declaring
    # zero. This is the rule inherited from trade_network_analysis.
    out, _ = aggregate_eu(frame([
        {"period": "2015", "reporterCode": "276", "reporterISO": "DEU",
         "partnerCode": "842", "partnerISO": "USA", "primaryValue": 10.0,
         "netWgt": None},
        {"period": "2015", "reporterCode": "56", "reporterISO": "BEL",
         "partnerCode": "842", "partnerISO": "USA", "primaryValue": 20.0,
         "netWgt": None},
    ]), members)
    assert out["netWgt"][0] is None, "an all-null group became a value"
    assert out["primaryValue"][0] == 30.0

    # ... but a partially declared group is summed over what is there.
    out, _ = aggregate_eu(frame([
        {"period": "2015", "reporterCode": "276", "reporterISO": "DEU",
         "partnerCode": "842", "partnerISO": "USA", "netWgt": 5.0},
        {"period": "2015", "reporterCode": "56", "reporterISO": "BEL",
         "partnerCode": "842", "partnerISO": "USA", "netWgt": None},
    ]), members)
    assert out["netWgt"][0] == 5.0, out["netWgt"][0]

    # Quantities in different units are NOT added up: the unit is a key.
    out, _ = aggregate_eu(frame([
        {"period": "2015", "reporterCode": "276", "reporterISO": "DEU",
         "partnerCode": "842", "partnerISO": "USA", "qtyUnitCode": 8,
         "primaryValue": 1.0},
        {"period": "2015", "reporterCode": "56", "reporterISO": "BEL",
         "partnerCode": "842", "partnerISO": "USA", "qtyUnitCode": 12,
         "primaryValue": 2.0},
    ]), members)
    assert out.height == 2, "quantities in two units were merged"

    # A non-member is left strictly alone.
    out, _ = aggregate_eu(frame([
        {"period": "2015", "reporterCode": "842", "reporterISO": "USA",
         "partnerCode": "392", "partnerISO": "JPN", "primaryValue": 9.0},
    ]), members)
    assert out["reporterISO"][0] == "USA" and out["partnerISO"][0] == "JPN"

    # The per-declaration columns are dropped, and said so.
    _, report = aggregate_eu(frame([
        {"period": "2015", "reporterCode": "276", "reporterISO": "DEU",
         "partnerCode": "842", "partnerISO": "USA", "primaryValue": 1.0},
    ]), members)
    assert "isReported" in report["columns_dropped"], report["columns_dropped"]
    assert "reporterCode" in report["columns_dropped"]

    # The real membership file parses and resolves.
    real = eu_membership()
    assert real.height >= 28, real.height
    codes = set(real["code"])
    assert {"56", "58"} <= codes, "Belgium must carry both its codes"
    assert real.filter(pl.col("code") == "826")["to"][0] == 2020, "GBR to 2020"

    print("comtrade_polars: checks OK")


if __name__ == "__main__":
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    _demo()
