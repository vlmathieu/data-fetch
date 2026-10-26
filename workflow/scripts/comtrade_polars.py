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

from comtrade_common import finding, raise_on_fail

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


# --- pseudo-country filter --------------------------------------------------
# The sides the filter looks at. partner2Code is DELIBERATELY ABSENT, and that
# absence is the whole point of this constant existing:
#
# when partner2 is not requested, Comtrade fills partner2Code with 0 on every
# row, and 0 is the code of "World". Filtering on it dropped 16,934 rows out of
# 16,934 on the first real download (2026-10-25). The 0 there is a
# "not applicable" placeholder, not an aggregate.
#
# Keyed on the CODE columns and not the ISO ones, because include_desc: false
# delivers no ISO columns at all.
PSEUDO_SIDES = ("reporterCode", "partnerCode")

# Sides the "is this code known at all?" check looks at, and the asymmetry
# with PSEUDO_SIDES above is DELIBERATE. For the pseudo filter, partner2Code
# is a trap: it holds 0 on every row when partner2 is not requested, and 0 is
# World's code. Here 0 is simply a code the table KNOWS, so it cannot produce
# a false positive -- while an unknown partner2 code would be a real blind
# spot. Hence partner2Code belongs in this tuple and not in that one.
AREA_SIDES = PSEUDO_SIDES + ("partner2Code",)


def drop_pseudo(data: pl.DataFrame, pseudo: frozenset[str] | set[str]
                ) -> tuple[pl.DataFrame, dict]:
    """Drops the rows whose reporter or partner is not a country.

    Parameters
    ----------
    data : polars dataframe
        Rows as received from the API, holding at least one of PSEUDO_SIDES.
    pseudo : set of str
        Numeric codes to drop, as strings. From comtrade_common.pseudo_codes():
        aggregates and territories with no ISO3, Taiwan (490) excluded.

    Returns
    -------
    (kept, report) : tuple
        kept : polars dataframe
            The rows whose every checked side is a real country.
        report : dict
            'rows_in', 'rows_out', 'dropped' ({'partnerCode=0': 1129, ...}),
            so the log and the sidecar can name what went and why.
    """
    sides = [c for c in PSEUDO_SIDES if c in data.columns]
    dropped: dict[str, int] = {}
    for side in sides:
        counts = (
            data.filter(pl.col(side).cast(pl.String).is_in(pseudo))
            .group_by(side).len().sort("len", descending=True)
        )
        for code, n in counts.iter_rows():
            dropped[f"{side}={code}"] = n

    is_pseudo = pl.any_horizontal(
        [pl.col(c).cast(pl.String).is_in(pseudo) for c in sides]
    )
    kept = data.filter(~is_pseudo)
    return kept, {"rows_in": data.height, "rows_out": kept.height,
                  "dropped": dropped}


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
        #
        # The cast is not cosmetic: Comtrade delivers the code columns as i64,
        # while the membership codes are strings, and polars refuses a join
        # across the two. Caught on the first real run (2026-10-25).
        data.with_columns(pl.col(f"{side}Code").cast(pl.String).alias("_code"))
        .join(members, left_on="_code", right_on="code", how="left")
        .with_columns(
            (pl.col("from").is_not_null()
             & (pl.col("year") >= pl.col("from"))
             & (pl.col("year") <= pl.col("to"))).alias(flag)
        )
        .drop("_code", "from", "to", "code", strict=False)
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
            'intra_eu_dropped', 'intra_eu_value', 'columns_dropped',
            'rows_in', 'rows_out', 'rows_touching_eu'. Logged by the caller,
            so nothing disappears silently.
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
    intra_rows = data.filter(both)
    intra = intra_rows.height
    # The value deleted, not only the rows: it is the right-hand side of the
    # conservation equation check_eu verifies.
    intra_value = (intra_rows["primaryValue"].sum()
                   if "primaryValue" in data.columns else None)
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
        "intra_eu_value": intra_value,
        "columns_dropped": dropped,
        "rows_touching_eu": result.filter(
            (pl.col("reporterISO") == EU_ISO)
            | (pl.col("partnerISO") == EU_ISO)
        ).height,
    }


# --- download checks --------------------------------------------------------
# What these guard against is NOT a crash: a crash is loud and cheap. It is a
# file that is plausible but wrong -- it opens, the dtypes are right, the row
# count looks reasonable, and nobody notices until it is published. Both bugs
# found on this repo so far were of that family, and both were found by a real
# run rather than by rereading the code.
#
# Every check returns findings instead of raising, so that ONE run reports
# everything that is off rather than stopping at the first problem. The caller
# logs them, then calls raise_on_fail().

# Columns without which the file cannot be interpreted at all. Code columns
# only: include_desc: false delivers no ISO and no Desc column.
REQUIRED = ("typeCode", "freqCode", "period", "reporterCode", "partnerCode",
            "flowCode", "classificationCode", "cmdCode", "primaryValue")

# Additionally required when include_desc is true. Checking them is what makes
# the ISO3 double-count check unconditional: the version of that check written
# inside the download script was wrapped in `if "reporterISO" in data.columns`,
# so a renamed column would have switched the guard off in silence.
REQUIRED_DESC = ("reporterISO", "partnerISO", "reporterDesc", "partnerDesc")

# What identifies one declaration. A duplicate on this key is unambiguously a
# bug: two packs overlapping on a period, or bisect_codes handing out two
# non-disjoint code subsets, would duplicate rows that are each individually
# valid, in a file whose total looks plausible -- and every analysis downstream
# would count them twice.
#
# Code columns only, same reason as REQUIRED. Measured on the real download of
# 2026-10-26: 0 duplicates on 15,630 rows, and every subset of this key down to
# five columns was still unique, so the key is wide enough not to raise a false
# alarm and narrow enough to catch an exact duplicate.
ROW_KEYS = ("period", "flowCode", "reporterCode", "partnerCode",
            "partner2Code", "cmdCode", "classificationCode",
            "customsCode", "mosCode", "motCode",
            "qtyUnitCode", "altQtyUnitCode")

# Relative tolerance for the conservation checks. Sums of f64 over tens of
# thousands of rows do not come back bit-identical, so an exact == would fail
# on arithmetic rather than on a bug.
#
# 1e-12 and not something looser: on the real total of 6.7e10 that leaves 0.07
# of slack, while the accumulation error of summing ~16,000 f64 values is
# around 1e-3. A looser 1e-9 would have allowed 67 units of drift, enough to
# hide a whole lost row -- caught by the self-check below, which is why the
# number is this one.
TOLERANCE = 1e-12


def _check_schema(data: pl.DataFrame, params: dict) -> list[dict]:
    """Are the columns the rest of the pipeline reads actually there?

    Parameters
    ----------
    data : polars dataframe
        Rows as received from the API, before any filtering.
    params : dict
        plan['params_resolved']; only 'include_desc' is read.

    Returns
    -------
    list of dict
        One finding, 'fail' if a required column is missing.
    """
    expected = list(REQUIRED)
    if params["include_desc"]:
        expected += list(REQUIRED_DESC)
    missing = [c for c in expected if c not in data.columns]
    if missing:
        return [finding(
            "schema", "fail",
            f"columns missing from the response: {missing}. The API changed, "
            f"or the request did not ask for what we think it did.",
            missing=missing,
        )]
    return [finding("schema", "pass",
                     f"{len(expected)} required column(s) present, "
                     f"{data.width} in all")]


def _check_counts(data: pl.DataFrame, plan: dict) -> list[dict]:
    """Rows received against the rows the free preflight counted.

    MUST run before drop_pseudo. The preflight counts are pre-filter by
    design, so comparing after the filter reports a deficit on every period of
    a perfectly healthy run: measured at -326 to -328 per period on
    2026-10-26.

    'warn' and not 'fail', deliberately: Comtrade rewrites history, so a gap
    between the preflight and the download can be a legitimate revision.
    Telling the two apart needs datasetChecksum, which is not checked here, and
    blocking a workflow on a revision would be worse than warning about it.

    Parameters
    ----------
    data : polars dataframe
        Rows as received, before any filtering.
    plan : dict
        The plan.json, read for 'counts' and 'expected_rows_total'.

    Returns
    -------
    list of dict
        Two findings: 'counts_period' and 'counts_total'.
    """
    findings = []

    got = dict(
        data.select(pl.col("period").cast(pl.String))
        .group_by("period").len().iter_rows()
    )
    gaps = {
        period: {"expected": expected, "received": got.get(period, 0)}
        for period, expected in plan["counts"].items()
        if got.get(period, 0) != expected
    }
    if gaps:
        worst = ", ".join(f"{p} {g['received']}/{g['expected']}"
                          for p, g in list(gaps.items())[:5])
        findings.append(finding(
            "counts_period", "warn",
            f"{len(gaps)} period(s) differ from the preflight count: "
            f"{worst}. A Comtrade revision between preflight and download "
            f"does this; so does a bug. datasetChecksum settles it.",
            gaps=gaps,
        ))
    else:
        findings.append(finding(
            "counts_period", "pass",
            f"{len(plan['counts'])} period(s) match the preflight count"))

    expected_total = plan["expected_rows_total"]
    if data.height != expected_total:
        findings.append(finding(
            "counts_total", "warn",
            f"{data.height} rows received, {expected_total} counted by the "
            f"preflight ({data.height - expected_total:+d})",
            expected=expected_total, received=data.height))
    else:
        findings.append(finding(
            "counts_total", "pass", f"{data.height} rows, as counted",
            expected=expected_total, received=data.height))
    return findings


def _check_request(data: pl.DataFrame, params: dict) -> list[dict]:
    """Is what came back what was asked for?

    The failure this exists for: being delivered another classification,
    another frequency or another product, without a word. Each one makes the
    file silently answer a different question from the one asked.

    Parameters
    ----------
    data : polars dataframe
        Rows as received, before any filtering.
    params : dict
        plan['params_resolved'].

    Returns
    -------
    list of dict
        Findings 'classification', 'frequency', 'flow' and 'cmd'.
    """
    from comtrade_common import FIXED  # noqa: PLC0415

    findings = []

    # --- classification ---
    # NOT an equality when 'HS' is asked. 'HS' means "the revision in force
    # that year", and each declarant has its own: measured on 2026-10-26,
    # 2000-2003 in HS comes back as H0, H1 AND H2, with all three inside 2002
    # alone. What would be wrong is an S1 or a BEC.
    requested = params["classification"]
    delivered = sorted(data["classificationCode"].unique().to_list())
    if requested == "HS":
        stray = [c for c in delivered
                 if not (len(c) == 2 and c[0] == "H" and c[1].isdigit())]
        findings.append(
            finding("classification", "fail",
                     f"HS asked, non-HS classification(s) delivered: {stray}",
                     delivered=delivered)
            if stray else
            finding("classification", "pass",
                     f"HS -> {', '.join(delivered)} (the revision in force "
                     f"each year)", delivered=delivered)
        )
    else:
        # shortcut: a forced revision is only WARNED about, because what the
        # API labels converted rows with is unmeasured. The public preview
        # cannot settle it: asked on 2026-10-26, clCode H2 returns
        # {"error":"Something went wrong"} and H3 returns 0 rows, while HS on
        # the same call shape returns 500 rows. Upgrade to 'fail' once a real
        # forced-revision download has been seen.
        wrong = [c for c in delivered if c != requested]
        findings.append(
            finding("classification", "warn",
                     f"{requested} asked, delivered: {delivered}. Unmeasured "
                     f"whether UNSD labels converted rows with the target "
                     f"revision or the source one.", delivered=delivered)
            if wrong else
            finding("classification", "pass", f"{requested} as asked",
                     delivered=delivered)
        )

    # --- frequency and type ---
    freq = sorted(data["freqCode"].unique().to_list())
    kind = sorted(data["typeCode"].unique().to_list())
    if freq != [params["freq"]] or kind != [FIXED["type"]]:
        findings.append(finding(
            "frequency", "fail",
            f"freqCode {freq} / typeCode {kind} delivered, "
            f"['{params['freq']}'] / ['{FIXED['type']}'] asked"))
    else:
        findings.append(finding("frequency", "pass",
                                 f"freqCode {freq}, typeCode {kind}"))

    # --- flow ---
    # 'warn': mirror_flows already logs the flows it ignores, and a selection
    # may legitimately bring back RX or RM. Compared to what was asked, not to
    # a hard-coded {M, X}.
    asked_flow = set(params["flow"])
    got_flow = set(data["flowCode"].unique().to_list())
    extra_flow = sorted(got_flow - asked_flow)
    findings.append(
        finding("flow", "warn",
                 f"flow code(s) delivered that were not asked for: "
                 f"{extra_flow} (asked: {sorted(asked_flow)})",
                 extra=extra_flow)
        if extra_flow else
        finding("flow", "pass", f"flowCode {sorted(got_flow)}")
    )

    # --- products ---
    asked_cmd = set(params["cmd"])
    got_cmd = set(data["cmdCode"].cast(pl.String).unique().to_list())
    extra_cmd = sorted(got_cmd - asked_cmd)
    findings.append(
        finding("cmd", "fail",
                 f"product code(s) delivered that were not asked for: "
                 f"{extra_cmd[:10]} ({len(extra_cmd)} in all). The selection "
                 f"did not reach the API as intended.", extra=extra_cmd)
        if extra_cmd else
        finding("cmd", "pass",
                 f"{len(got_cmd)} product code(s), all asked for")
    )
    return findings


def _check_periods(data: pl.DataFrame, params: dict) -> list[dict]:
    """Periods delivered against periods asked for, both ways.

    Two sides of one coin, and only one of them was checked before: a missing
    period was detected, a period IN EXCESS never was -- and that one would
    mean a pack parameter went out wrong.

    Parameters
    ----------
    data : polars dataframe
        Rows as received, before any filtering. Before, because after it a
        period can legitimately disappear if every one of its rows was a
        pseudo-country.
    params : dict
        plan['params_resolved'], read for 'period'.

    Returns
    -------
    list of dict
        Findings 'periods_extra' (fail) and 'periods_missing' (warn).
    """
    asked = set(params["period"])
    got = set(data.select(pl.col("period").cast(pl.String))["period"].unique())

    extra = sorted(got - asked)
    missing = sorted(asked - got)
    return [
        finding("periods_extra", "fail",
                 f"periods delivered that were not asked for: {extra}",
                 extra=extra)
        if extra else
        finding("periods_extra", "pass",
                 f"{len(got)} period(s), all asked for"),

        finding("periods_missing", "warn",
                 f"periods asked for and absent from the result: {missing}",
                 missing=missing)
        if missing else
        finding("periods_missing", "pass", "no period missing"),
    ]


def _check_iso3(data: pl.DataFrame, params: dict) -> list[dict]:
    """One ISO3 under two reporter codes in one period double counts a country.

    The historical splits (FRA 250/251, BEL 56/58) are disjoint in time, but
    that is measured on six pairs out of fifteen, not guaranteed by UNSD. So
    it is checked on the real rows rather than assumed.

    Parameters
    ----------
    data : polars dataframe
        Rows as received, before any filtering.
    params : dict
        plan['params_resolved']; the check is skipped unless include_desc,
        which is the only case where the ISO columns exist.

    Returns
    -------
    list of dict
        One finding, 'warn' on a collision.
    """
    if not params["include_desc"]:
        return [finding("iso3_unique", "pass",
                         "skipped: no ISO column without include_desc")]

    collisions = (
        data.select("period", "reporterISO", "reporterCode")
        .unique()
        .group_by("period", "reporterISO")
        .len()
        .filter(pl.col("len") > 1)
        .sort("period", "reporterISO")
    )
    if collisions.height:
        pairs = [f"{iso} in {period} ({n} codes)"
                 for period, iso, n in collisions.iter_rows()]
        return [finding("iso3_unique", "warn",
                         f"possible double count: {'; '.join(pairs)}",
                         collisions=pairs)]
    return [finding("iso3_unique", "pass",
                     "no ISO3 under two reporter codes in one period")]


def _check_known_areas(data: pl.DataFrame) -> list[dict]:
    """Does the areas table know every code that was delivered?

    `resources/comtrade_areas.json` is versioned on purpose, so it drifts. An
    area UNSD added since the last build_comtrade_areas.py run is in neither
    set: not in pseudo_codes(), so exclude_pseudo leaves it in, and not in the
    table, so nothing knows what it is. It would be treated as a country in
    silence. This is the only thing that would say otherwise.

    'warn' and not 'fail', and the reason matters: an unknown code does not
    describe a corrupt file, it describes a STALE LOCAL TABLE. The data is
    fine, our classification of it has fallen behind, and refusing a download
    because resources/ is six months old would be the wrong call.

    Parameters
    ----------
    data : polars dataframe
        Rows as received, before any filtering.

    Returns
    -------
    list of dict
        One finding, 'warn' naming the unknown codes and the remedy.
    """
    from comtrade_common import area_codes  # noqa: PLC0415

    known = area_codes()
    sides = [c for c in AREA_SIDES if c in data.columns]
    unknown: dict[str, list[str]] = {}
    for side in sides:
        codes = set(data[side].cast(pl.String).unique().to_list()) - known
        if codes:
            unknown[side] = sorted(codes)

    if unknown:
        return [finding(
            "known_areas", "warn",
            f"code(s) absent from resources/comtrade_areas.json: {unknown}. "
            f"UNSD has probably added an area since the table was built: it "
            f"was NOT filtered by exclude_pseudo and nothing knows whether it "
            f"is a country. Refresh with `python "
            f"workflow/scripts/build_comtrade_areas.py` (public, no key).",
            unknown=unknown)]
    return [finding("known_areas", "pass",
                    f"every code on {len(sides)} side(s) is in the table",
                    known_total=len(known))]


def check_csv(path: str, data: pl.DataFrame, separator: str) -> list[dict]:
    """Reads the delivered CSV back and compares it to what was written.

    The CSV is the artifact that goes to colleagues in Stata or R, and until
    now it was the one nothing verified. Two claims about it were written in
    prose and never checked -- in particular that polars quotes a field
    containing the separator, so that "China, Hong Kong SAR" survives a comma.
    That is exactly the kind of claim that produced the last two bugs here.

    Read back with every column as a string: this checks the TRANSPORT, not
    the types. Leading zeros are deliberately NOT compared -- they are lost on
    reading a CSV back, which is known, documented, and why the parquet is the
    canonical artifact. A strict equality would fail on intended behaviour.

    Parameters
    ----------
    path : str
        The CSV just written.
    data : polars dataframe
        The frame it was written from.
    separator : str
        The separator used, from the config's csv_sep.

    Returns
    -------
    list of dict
        One finding. 'fail' if the file does not read back to the same shape
        or the same total; its detail always says how many fields contained
        the separator, so a vacuous pass is visible as such.
    """
    # How many fields would need quoting at all. Reported even on a pass: if
    # this is 0, the read-back proved nothing about quoting, and the detail
    # says so rather than implying a guarantee.
    quoted = sum(
        data.select(
            pl.col(c).cast(pl.String).str.contains(separator, literal=True)
            .sum()
        ).item() or 0
        for c, dtype in data.schema.items() if dtype == pl.String
    )

    try:
        back = pl.read_csv(path, separator=separator, infer_schema_length=0)
    except Exception as exc:                          # noqa: BLE001
        return [finding("csv_roundtrip", "fail",
                        f"the CSV just written cannot be read back: "
                        f"{type(exc).__name__}: {exc}")]

    if (back.height, back.width) != (data.height, data.width):
        return [finding(
            "csv_roundtrip", "fail",
            f"the CSV reads back as {back.height} rows x {back.width} columns "
            f"instead of {data.height} x {data.width}: fields were shifted, "
            f"which a broken quote around one of the {quoted} value(s) "
            f"containing '{separator}' would do.",
            rows=back.height, columns=back.width, quoted_fields=quoted)]

    if "primaryValue" in data.columns:
        written = data["primaryValue"].sum()
        read = back["primaryValue"].cast(pl.Float64, strict=False).sum()
        if not _conserved(written, read):
            return [finding(
                "csv_roundtrip", "fail",
                f"primaryValue sums to {read} in the CSV against {written} in "
                f"the frame: values were truncated or shifted.",
                expected=written, received=read)]

    return [finding(
        "csv_roundtrip", "pass",
        f"{back.height} rows x {back.width} columns read back identical, "
        + (f"{quoted} field(s) containing '{separator}' included"
           if quoted else
           f"but NO field contained '{separator}', so the quoting was not "
           f"exercised"),
        rows=back.height, columns=back.width, quoted_fields=quoted)]


def _check_duplicates(data: pl.DataFrame) -> list[dict]:
    """No declaration may appear twice.

    Parameters
    ----------
    data : polars dataframe
        Rows as received, before any filtering.

    Returns
    -------
    list of dict
        One finding, 'fail' with the number of surplus rows.
    """
    keys = [c for c in ROW_KEYS if c in data.columns]
    extra = data.height - data.select(keys).unique().height
    if extra:
        worst = (
            data.group_by(keys).len().filter(pl.col("len") > 1)
            .sort("len", descending=True).head(3)
        )
        return [finding(
            "duplicates", "fail",
            f"{extra} surplus row(s) on the declaration key: the same "
            f"declaration was received more than once, which double counts it "
            f"in every analysis downstream. First group(s): "
            f"{worst.to_dicts()}",
            extra_rows=extra, keys=keys,
        )]
    return [finding("duplicates", "pass", "no duplicate declaration",
                     extra_rows=0, keys=keys)]


def check_download(data: pl.DataFrame, plan: dict) -> list[dict]:
    """Every check on a freshly downloaded dataset, before it is filtered.

    Call this between the concat of the packs and drop_pseudo, and before any
    write: the counts are pre-filter, and a failing check must not leave a file
    behind.

    Parameters
    ----------
    data : polars dataframe
        Rows as received from the API, packs concatenated, nothing filtered.
    plan : dict
        The plan.json produced by comtrade_preflight. Passed whole rather than
        as separate arguments so that the counts and the resolved request
        cannot come from two different runs.

    Returns
    -------
    list of dict
        Findings, in check order. The schema check comes first and SHORT
        CIRCUITS: with a required column missing, every later check would
        either crash or report nonsense.
    """
    params = plan["params_resolved"]

    findings = _check_schema(data, params)
    if findings[0]["status"] == "fail":
        return findings

    findings += _check_counts(data, plan)
    findings += _check_request(data, params)
    findings += _check_periods(data, params)
    findings += _check_iso3(data, params)
    findings += _check_known_areas(data)
    findings += _check_duplicates(data)
    return findings


def _conserved(left: float | None, right: float | None) -> bool:
    """Are two sums equal within TOLERANCE, treating null as 0.0?"""
    a, b = float(left or 0.0), float(right or 0.0)
    return abs(a - b) <= TOLERANCE * max(1.0, abs(a), abs(b))


def check_mirror(source: pl.DataFrame, mirror: pl.DataFrame) -> list[dict]:
    """The mirror must move value around, never lose any.

    Catches a row silently dropped by the group_by or the full join. The two
    declarations are never reconciled, so each side is checked against its own
    flow direction only.

    Compared on NON-NULL values: guarded_sum keeps a null group null, and
    polars' sum() skips nulls, so any other comparison would fail on behaviour
    that is deliberate.

    Parameters
    ----------
    source : polars dataframe
        The canonical parquet the mirror was built from.
    mirror : polars dataframe
        The mirror, with its '_exp' and '_imp' columns.

    Returns
    -------
    list of dict
        One finding per side present, 'fail' if value went missing.
    """
    findings = []
    for flow, suffix, role in (("X", "exp", "exporter"),
                               ("M", "imp", "importer")):
        column = f"primaryValue_{suffix}"
        if (column not in mirror.columns
                or "primaryValue" not in source.columns):
            continue
        expected = (source.filter(pl.col("flowCode") == flow)["primaryValue"]
                    .sum())
        received = mirror[column].sum()
        findings.append(
            finding(f"mirror_{suffix}", "pass",
                     f"{role} value conserved: {received}",
                     expected=expected, received=received)
            if _conserved(expected, received) else
            finding(f"mirror_{suffix}", "fail",
                     f"{role} value NOT conserved: {received} in the mirror "
                     f"against {expected} in the {flow} rows of the source. "
                     f"Rows were lost in the group_by or the join.",
                     expected=expected, received=received)
        )
    return findings


def check_eu(source: pl.DataFrame, result: pl.DataFrame, report: dict
             ) -> list[dict]:
    """The EU node must sum its members exactly, minus the intra-EU trade.

    The one arithmetic statement the aggregation makes: output == input minus
    what was deliberately deleted. Verified by hand on 2026-10-25
    (67,175,423,427 - 6,946,311,657 = 60,229,111,770, gap 0.00); a check by
    hand is not a check.

    Parameters
    ----------
    source : polars dataframe
        The canonical parquet, before aggregation.
    result : polars dataframe
        The aggregated frame returned by aggregate_eu.
    report : dict
        Its report, read for 'intra_eu_value'.

    Returns
    -------
    list of dict
        One finding, 'fail' if value was created or lost.
    """
    if "primaryValue" not in source.columns:
        return []
    total = source["primaryValue"].sum()
    intra = report["intra_eu_value"]
    expected = float(total or 0.0) - float(intra or 0.0)
    received = result["primaryValue"].sum()
    return [
        finding("eu_conserved", "pass",
                 f"value conserved: {received} = {total} - {intra}",
                 expected=expected, received=received)
        if _conserved(expected, received) else
        finding("eu_conserved", "fail",
                 f"value NOT conserved: {received} in the EU file against "
                 f"{expected} expected ({total} raw - {intra} intra-EU). The "
                 f"aggregation created or lost value.",
                 expected=expected, received=received)
    ]


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
        """One trade row per dict, with the columns the aggregation reads.

        Codes are INTEGERS here, as Comtrade actually delivers them (i64).
        The first version of this fixture used strings, so the join against the
        membership table passed the check and failed on the real file.
        """
        base = {"typeCode": "C", "freqCode": "A", "flowCode": "X",
                "classificationCode": "H5", "cmdCode": "4403",
                "qtyUnitCode": 8, "altQtyUnitCode": None,
                "reporterDesc": "x", "partnerDesc": "y",
                "isReported": True, "netWgt": None, "primaryValue": None}
        return pl.DataFrame([{**base, **row} for row in rows],
                            infer_schema_length=None)

    # Two members declaring towards the same non-member: one EU row.
    out, report = aggregate_eu(frame([
        {"period": "2015", "reporterCode": 276, "reporterISO": "DEU",
         "partnerCode": 842, "partnerISO": "USA", "primaryValue": 100.0},
        {"period": "2015", "reporterCode": 56, "reporterISO": "BEL",
         "partnerCode": 842, "partnerISO": "USA", "primaryValue": 30.0},
    ]), members)
    assert out.height == 1, out
    assert out["reporterISO"][0] == EU_ISO
    assert out["reporterDesc"][0] == EU_DESC
    assert out["primaryValue"][0] == 130.0, out["primaryValue"][0]
    assert report["intra_eu_dropped"] == 0

    # Intra-EU trade is deleted, not summed into the node.
    out, report = aggregate_eu(frame([
        {"period": "2015", "reporterCode": 276, "reporterISO": "DEU",
         "partnerCode": 56, "partnerISO": "BEL", "primaryValue": 50.0},
        {"period": "2015", "reporterCode": 276, "reporterISO": "DEU",
         "partnerCode": 842, "partnerISO": "USA", "primaryValue": 100.0},
    ]), members)
    assert report["intra_eu_dropped"] == 1, report
    assert out.height == 1 and out["primaryValue"][0] == 100.0, out

    # Dated membership, in both directions.
    out, _ = aggregate_eu(frame([
        {"period": "2019", "reporterCode": 826, "reporterISO": "GBR",
         "partnerCode": 842, "partnerISO": "USA", "primaryValue": 1.0},
        {"period": "2021", "reporterCode": 826, "reporterISO": "GBR",
         "partnerCode": 842, "partnerISO": "USA", "primaryValue": 2.0},
        {"period": "2012", "reporterCode": 191, "reporterISO": "HRV",
         "partnerCode": 842, "partnerISO": "USA", "primaryValue": 3.0},
        {"period": "2013", "reporterCode": 191, "reporterISO": "HRV",
         "partnerCode": 842, "partnerISO": "USA", "primaryValue": 4.0},
    ]), members)
    got = {(r["period"], r["reporterISO"]) for r in out.to_dicts()}
    assert ("2019", EU_ISO) in got, "GBR was an EU member in 2019"
    assert ("2021", "GBR") in got, "GBR left the EU before 2021"
    assert ("2012", "HRV") in got, "HRV joined in 2013, not before"
    assert ("2013", EU_ISO) in got, "HRV was a member in 2013"

    # REGRESSION, found on the first real EU run (2026-10-25): Comtrade
    # delivers the code columns as i64 while the membership codes are strings,
    # and polars refuses to join across the two. Both dtypes must work, since
    # a fixture written in strings is what hid the bug in the first place.
    for code in (276, "276"):
        out, _ = aggregate_eu(frame([
            {"period": "2015", "reporterCode": code, "reporterISO": "DEU",
             "partnerCode": 842, "partnerISO": "USA", "primaryValue": 1.0},
        ]), members)
        assert out["reporterISO"][0] == EU_ISO, f"{type(code).__name__} code failed"

    # The historical code of a member is folded in too: without code 58,
    # Belgium 1996-1998 would stay outside the EU node.
    out, _ = aggregate_eu(frame([
        {"period": "1996", "reporterCode": 58, "reporterISO": "BEL",
         "partnerCode": 842, "partnerISO": "USA", "primaryValue": 7.0},
    ]), members)
    assert out["reporterISO"][0] == EU_ISO, out

    # Nulls stay null: an all-null group must not become a reporter declaring
    # zero. This is the rule inherited from trade_network_analysis.
    out, _ = aggregate_eu(frame([
        {"period": "2015", "reporterCode": 276, "reporterISO": "DEU",
         "partnerCode": 842, "partnerISO": "USA", "primaryValue": 10.0,
         "netWgt": None},
        {"period": "2015", "reporterCode": 56, "reporterISO": "BEL",
         "partnerCode": 842, "partnerISO": "USA", "primaryValue": 20.0,
         "netWgt": None},
    ]), members)
    assert out["netWgt"][0] is None, "an all-null group became a value"
    assert out["primaryValue"][0] == 30.0

    # ... but a partially declared group is summed over what is there.
    out, _ = aggregate_eu(frame([
        {"period": "2015", "reporterCode": 276, "reporterISO": "DEU",
         "partnerCode": 842, "partnerISO": "USA", "netWgt": 5.0},
        {"period": "2015", "reporterCode": 56, "reporterISO": "BEL",
         "partnerCode": 842, "partnerISO": "USA", "netWgt": None},
    ]), members)
    assert out["netWgt"][0] == 5.0, out["netWgt"][0]

    # Quantities in different units are NOT added up: the unit is a key.
    out, _ = aggregate_eu(frame([
        {"period": "2015", "reporterCode": 276, "reporterISO": "DEU",
         "partnerCode": 842, "partnerISO": "USA", "qtyUnitCode": 8,
         "primaryValue": 1.0},
        {"period": "2015", "reporterCode": 56, "reporterISO": "BEL",
         "partnerCode": 842, "partnerISO": "USA", "qtyUnitCode": 12,
         "primaryValue": 2.0},
    ]), members)
    assert out.height == 2, "quantities in two units were merged"

    # A non-member is left strictly alone.
    out, _ = aggregate_eu(frame([
        {"period": "2015", "reporterCode": 842, "reporterISO": "USA",
         "partnerCode": 392, "partnerISO": "JPN", "primaryValue": 9.0},
    ]), members)
    assert out["reporterISO"][0] == "USA" and out["partnerISO"][0] == "JPN"

    # The per-declaration columns are dropped, and said so.
    _, report = aggregate_eu(frame([
        {"period": "2015", "reporterCode": 276, "reporterISO": "DEU",
         "partnerCode": 842, "partnerISO": "USA", "primaryValue": 1.0},
    ]), members)
    assert "isReported" in report["columns_dropped"], report["columns_dropped"]
    assert "reporterCode" in report["columns_dropped"]

    # --- pseudo-country filter ---
    pseudo = {"0", "97", "899", "837"}       # World, EU, Areas nes, Bunkers

    def rows(**over) -> pl.DataFrame:
        """Two trade rows, overridable, with the columns the filter reads."""
        base = {"reporterCode": 251, "partnerCode": 842, "partner2Code": 0,
                "primaryValue": 1.0}
        return pl.DataFrame([{**base, **over}, {**base, **over}])

    # REGRESSION, found on the first real download (2026-10-25): partner2Code
    # is 0 on EVERY row when partner2 is not requested, and 0 is World's code.
    # Filtering on it emptied the whole dataset, 16,934 rows out of 16,934.
    kept, report = drop_pseudo(rows(), pseudo)
    assert kept.height == 2, "partner2Code=0 must not drop a row"
    assert report["dropped"] == {}, report["dropped"]

    # A real aggregate on either side does go.
    kept, report = drop_pseudo(rows(partnerCode=0), pseudo)
    assert kept.height == 0 and report["dropped"] == {"partnerCode=0": 2}
    kept, _ = drop_pseudo(rows(reporterCode=97), pseudo)
    assert kept.height == 0, "the EU aggregate reporter must go"

    # Taiwan stays: it is not in the pseudo set.
    assert drop_pseudo(rows(partnerCode=490), pseudo)[0].height == 2

    # Mixed rows: only the offending ones go, and the report counts them.
    mixed = pl.concat([rows(), rows(partnerCode=899)])
    kept, report = drop_pseudo(mixed, pseudo)
    assert (kept.height, report["rows_in"]) == (2, 4), (kept.height, report)
    assert report["dropped"] == {"partnerCode=899": 2}

    # No ISO columns (include_desc: false) must still work: codes only.
    no_iso = rows().drop("partner2Code")
    assert drop_pseudo(no_iso, pseudo)[0].height == 2

    # The real membership file parses and resolves.
    real = eu_membership()
    assert real.height >= 28, real.height
    codes = set(real["code"])
    assert {"56", "58"} <= codes, "Belgium must carry both its codes"
    assert real.filter(pl.col("code") == "826")["to"][0] == 2020, "GBR to 2020"

    # --- download checks ---
    # Every check gets a deliberately BROKEN dataset as well as a healthy one.
    # A check that is only ever shown a good file proves nothing: it would pass
    # just as happily if its body were `return []`.
    #
    # Dtypes copied from the real response of 2026-10-26, not invented: codes
    # are i64, period and cmdCode are strings, motCode is i64 while customsCode
    # and mosCode are strings. The two bugs this repo has had both came from a
    # fixture whose schema differed from the API's.
    def row(**over) -> dict:
        """One row carrying every column the checks read."""
        base = {"typeCode": "C", "freqCode": "A", "period": "2000",
                "reporterCode": 251, "reporterISO": "FRA",
                "reporterDesc": "France", "flowCode": "M",
                "partnerCode": 276, "partnerISO": "DEU",
                "partnerDesc": "Germany", "partner2Code": 0,
                "classificationCode": "H1", "cmdCode": "4403",
                "customsCode": "C00", "mosCode": "0", "motCode": 0,
                "qtyUnitCode": 8, "altQtyUnitCode": -1,
                "primaryValue": 100.0}
        return {**base, **over}

    SCHEMA = {"typeCode": pl.String, "freqCode": pl.String,
              "period": pl.String, "reporterCode": pl.Int64,
              "reporterISO": pl.String, "reporterDesc": pl.String,
              "flowCode": pl.String, "partnerCode": pl.Int64,
              "partnerISO": pl.String, "partnerDesc": pl.String,
              "partner2Code": pl.Int64, "classificationCode": pl.String,
              "cmdCode": pl.String, "customsCode": pl.String,
              "mosCode": pl.String, "motCode": pl.Int64,
              "qtyUnitCode": pl.Int64, "altQtyUnitCode": pl.Int64,
              "primaryValue": pl.Float64}

    def trade(rows: list[dict]) -> pl.DataFrame:
        return pl.DataFrame(rows, schema=SCHEMA)

    # Two periods, two partners: a plausible miniature of a real download.
    good = trade([
        row(period="2000", partnerCode=276, partnerISO="DEU"),
        row(period="2000", partnerCode=56, partnerISO="BEL"),
        row(period="2001", partnerCode=276, partnerISO="DEU",
            classificationCode="H2"),
    ])
    good_plan = {
        "params_resolved": {"classification": "HS", "freq": "A",
                            "flow": ["M", "X"], "cmd": ["4403"],
                            "period": ["2000", "2001"], "include_desc": True},
        "counts": {"2000": 2, "2001": 1},
        "expected_rows_total": 3,
    }

    def status_of(findings: list[dict], name: str) -> str:
        """The status of one named check, or '' if it was not run."""
        return next((f["status"] for f in findings if f["check"] == name), "")

    # A healthy dataset: everything passes, and nothing is left out.
    findings = check_download(good, good_plan)
    assert all(f["status"] == "pass" for f in findings), findings
    assert {f["check"] for f in findings} == {
        "schema", "counts_period", "counts_total", "classification",
        "frequency", "flow", "cmd", "periods_extra", "periods_missing",
        "iso3_unique", "known_areas", "duplicates"}, \
        {f["check"] for f in findings}
    # HS delivering several revisions is NORMAL, and the pass says which.
    assert "H1, H2" in next(f for f in findings
                            if f["check"] == "classification")["detail"]
    raise_on_fail(findings, "demo")          # must not raise

    # B2, schema: a missing required column stops everything, because every
    # later check would either crash or report nonsense on it.
    broken = check_download(good.drop("primaryValue"), good_plan)
    assert broken[0]["check"] == "schema" and broken[0]["status"] == "fail"
    assert len(broken) == 1, "the schema check must short circuit"
    try:
        raise_on_fail(broken, "demo")
    except RuntimeError as exc:
        assert "schema" in str(exc) and "nothing written" in str(exc)
    else:
        raise AssertionError("a failed check must raise")

    # include_desc: false drops the ISO columns legitimately, and then the
    # ISO3 collision check has nothing to look at rather than being skipped
    # in silence.
    no_desc_plan = {**good_plan,
                    "params_resolved": {**good_plan["params_resolved"],
                                        "include_desc": False}}
    no_desc = check_download(
        good.drop("reporterISO", "partnerISO", "reporterDesc", "partnerDesc"),
        no_desc_plan)
    assert all(f["status"] == "pass" for f in no_desc), no_desc

    # C, duplicates: the same declaration received twice. The file would open,
    # the dtypes would be right, and every total downstream would be wrong.
    doubled = pl.concat([good, good.head(1)])
    found = _check_duplicates(doubled)
    assert found[0]["status"] == "fail" and found[0]["extra_rows"] == 1, found
    # Two rows differing by ONE key column are not duplicates.
    one_key_apart = trade([row(), row(motCode=1)])
    assert _check_duplicates(one_key_apart)[0]["status"] == "pass"

    # A2, counts: a gap is warned about, never fatal -- a Comtrade revision
    # between the preflight and the download produces exactly this.
    wrong_counts = {**good_plan, "counts": {"2000": 99, "2001": 1},
                    "expected_rows_total": 100}
    found = _check_counts(good, wrong_counts)
    assert [f["status"] for f in found] == ["warn", "warn"], found
    assert found[1]["expected"] == 100 and found[1]["received"] == 3
    # and a run holding such a warning still goes through.
    raise_on_fail(check_download(good, wrong_counts), "demo")

    # D1, classification: HS tolerates any H revision and refuses the rest.
    assert status_of(check_download(
        trade([row(classificationCode="S1")]),
        {**good_plan, "counts": {"2000": 1}, "expected_rows_total": 1},
    ), "classification") == "fail"
    # A forced revision only WARNS: what UNSD labels a converted row with is
    # unmeasured (see the shortcut note in _check_request).
    forced = {"params_resolved": {**good_plan["params_resolved"],
                                  "classification": "H5"},
              "counts": {"2000": 1}, "expected_rows_total": 1}
    assert status_of(check_download(trade([row()]), forced),
                     "classification") == "warn"
    assert status_of(check_download(trade([row(classificationCode="H5")]),
                                    forced), "classification") == "pass"

    # D2, frequency and type: an equality, because neither is ever a mix.
    one = {**good_plan, "counts": {"2000": 1}, "expected_rows_total": 1}
    assert status_of(check_download(trade([row(freqCode="M")]), one),
                     "frequency") == "fail"
    assert status_of(check_download(trade([row(typeCode="S")]), one),
                     "frequency") == "fail"

    # D3, flow: a warning, since mirror_flows already reports what it ignores.
    assert status_of(check_download(trade([row(flowCode="RX")]), one),
                     "flow") == "warn"

    # D4, products: a code nobody asked for means the selection did not reach
    # the API as intended.
    assert status_of(check_download(trade([row(cmdCode="4407")]), one),
                     "cmd") == "fail"

    # D5, periods, both ways: one in excess fails, one missing warns.
    found = _check_periods(trade([row(period="1999")]),
                          good_plan["params_resolved"])
    assert [f["status"] for f in found] == ["fail", "warn"], found
    assert found[0]["extra"] == ["1999"]
    assert found[1]["missing"] == ["2000", "2001"]

    # ISO3 collision: the same ISO3 under two reporter codes in one period
    # would count that country twice. FRA 250/251 are disjoint in time, which
    # is measured on six pairs out of fifteen, not guaranteed.
    found = _check_iso3(trade([row(reporterCode=250), row(reporterCode=251)]),
                        good_plan["params_resolved"])
    assert found[0]["status"] == "warn", found
    assert "FRA in 2000" in found[0]["detail"]

    # --- G, conservation in the derived files ---
    # Null stays null, so the sums must be compared on non-null values: these
    # helpers are checked against a frame that has both.
    assert _conserved(1e11, 1e11) and _conserved(None, 0.0)
    assert not _conserved(1e11, 1e11 + 1)

    source = trade([row(flowCode="X", primaryValue=10.0),
                    row(flowCode="X", primaryValue=5.0, partnerCode=56),
                    row(flowCode="M", primaryValue=7.0)])
    mirror_ok = pl.DataFrame({"primaryValue_exp": [10.0, 5.0],
                              "primaryValue_imp": [7.0]  * 2})
    # The exporter side adds up; the importer side has been given a row too
    # many on purpose, as a lost-or-duplicated row would look.
    found = {f["check"]: f["status"] for f in check_mirror(source, mirror_ok)}
    assert found == {"mirror_exp": "pass", "mirror_imp": "fail"}, found

    # EU: output == input minus the intra-EU value deleted. The equation the
    # aggregation makes, checked instead of trusted.
    eu_source = trade([row(primaryValue=100.0), row(primaryValue=30.0)])
    eu_result = trade([row(primaryValue=70.0)])
    assert check_eu(eu_source, eu_result,
                    {"intra_eu_value": 60.0})[0]["status"] == "pass"
    assert check_eu(eu_source, eu_result,
                    {"intra_eu_value": 0.0})[0]["status"] == "fail"

    # And the real aggregation now reports that value, so check_eu can be fed
    # from it: a count of rows would not have been enough.
    _, eu_report = aggregate_eu(frame([
        {"reporterCode": 276, "reporterISO": "DEU", "partnerCode": 56,
         "partnerISO": "BEL", "period": "2015", "primaryValue": 12.0},
        {"reporterCode": 276, "reporterISO": "DEU", "partnerCode": 842,
         "partnerISO": "USA", "period": "2015", "primaryValue": 5.0},
    ]), members)
    assert eu_report["intra_eu_value"] == 12.0, eu_report

    # --- known areas: the drift of a deliberately versioned table ---
    # The healthy fixture uses real codes (251 FRA, 276 DEU, 56 BEL, 0 World),
    # so it must pass against the real table.
    assert _check_known_areas(good)[0]["status"] == "pass"

    # A code UNSD could add tomorrow: not in the table, so exclude_pseudo
    # would NOT filter it and nothing would know whether it is a country.
    stray_area = trade([row(partnerCode=999_999)])
    found = _check_known_areas(stray_area)
    assert found[0]["status"] == "warn", found
    assert found[0]["unknown"] == {"partnerCode": ["999999"]}, found[0]
    # A warning, never a refusal: the data is fine, our table is stale.
    raise_on_fail(found, "demo")

    # partner2Code IS inspected here, unlike in the pseudo filter: its 0 is a
    # known code, so it cannot false-alarm, while an unknown value would be a
    # real blind spot.
    assert "partner2Code" in AREA_SIDES and "partner2Code" not in PSEUDO_SIDES
    assert _check_known_areas(
        trade([row(partner2Code=999_999)]))[0]["status"] == "warn"

    # --- the CSV read back ---
    import tempfile                                   # noqa: PLC0415

    with tempfile.TemporaryDirectory() as folder:
        # A label holding the separator, which is the claim being tested:
        # polars must quote it, and it must come back whole.
        tricky = trade([row(partnerDesc="China, Hong Kong SAR"),
                        row(partnerCode=56, partnerDesc="Belgium")])
        path = f"{folder}/t.csv"
        tricky.write_csv(path, separator=",")
        found = check_csv(path, tricky, ",")
        assert found[0]["status"] == "pass", found
        # The pass must say the quoting was actually exercised, otherwise it
        # is a vacuous green light.
        assert found[0]["quoted_fields"] >= 1, found[0]
        assert "containing ','" in found[0]["detail"], found[0]["detail"]

        # No field holds the separator: the check says so instead of implying
        # a guarantee it did not test.
        plain = trade([row(partnerDesc="Germany")])
        plain.write_csv(f"{folder}/p.csv", separator=",")
        found = check_csv(f"{folder}/p.csv", plain, ",")
        assert found[0]["status"] == "pass" and found[0]["quoted_fields"] == 0
        assert "NO field contained" in found[0]["detail"]

        # A truncated file: rows went missing between writing and reading.
        lines = Path(path).read_text().splitlines()
        Path(f"{folder}/short.csv").write_text("\n".join(lines[:-1]) + "\n")
        found = check_csv(f"{folder}/short.csv", tricky, ",")
        assert found[0]["status"] == "fail", found
        assert "rows" in found[0]["detail"]

        # A value quietly altered: same shape, different total.
        altered = "\n".join(
            line.replace(",100.0", ",1.0") if i else line
            for i, line in enumerate(lines)
        ) + "\n"
        Path(f"{folder}/altered.csv").write_text(altered)
        found = check_csv(f"{folder}/altered.csv", tricky, ",")
        assert found[0]["status"] == "fail", found
        assert "primaryValue" in found[0]["detail"]

        # Not a CSV at all.
        Path(f"{folder}/junk.csv").write_text('a,b\n"unclosed,1\n')
        junk = check_csv(f"{folder}/junk.csv", tricky, ",")
        assert junk[0]["status"] == "fail", junk

    print("comtrade_polars: checks OK")


if __name__ == "__main__":
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    _demo()
