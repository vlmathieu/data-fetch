# data-fetch

Dated, traceable, reproducible data downloads. Every run writes a timestamped
file under a unique name, together with a *sidecar* that records exactly what
was asked for and which version of the data was served.

One source so far: **UN Comtrade**. The repo is built to take others. One
source is one rules file plus one config block.

## Getting started

```zsh
# 1. Size the run without downloading anything or spending quota
snakemake preflight --sdm conda --cores 1

# 2. Read the plan, then download
snakemake --sdm conda --cores 1

# 3. Replay a specific run (reproducible)
snakemake --sdm conda --cores 1 --config run_id=20261024T120000
```

Preflight goes only through Comtrade's **public** endpoints: no key, no quota.
It is the one step you can run without risk.

## API key

Downloading Comtrade's data through the `getFinalData` function of the
`comtradeapicall` package requires a UN Comtrade subscription key. It does not
have to be a premium key: a **free** registered key opens the same endpoint.
Premium raises the ceilings and the daily quota, it does not unlock the
download.

The workflow reads the key from the environment variable **`comtrade_apikey`**,
and nowhere else. It appears in no configuration file.

### What each tier gives

`getFinalData` calls `https://comtradeapi.un.org/data/v1/get/`, which is closed
without a key. Preflight calls `https://comtradeapi.un.org/public/v1/preview/`
instead, which is open to anyone.

| Tier | Cost | Data API (`data/v1/get`) | Records per call | Calls per day |
|---|---|---|---|---|
| Public, no key | free | no access | preview only: 500 records, one period and one product per call | not stated |
| Basic Individual | free, registration | yes | **100,000** | 500 |
| Premium Individual | paid | yes | **250,000** | 5,000 |
| Premium Institutional | paid | yes | **250,000** | unlimited |

*Nota Bene 1*: The per-day figures come from the preview-API quota row of the
subscription plans page plus its sentence that a free subscription includes
"data API access for up to 500 calls/day"; the page states no separate Data API
quota per tier. And it lists an API rate limit of 1 to 5 calls per second
without mapping those values to tiers, which is part of why every call goes
through `call()` with backoff.

*Nota Bene 2*: Bulk files and the async endpoint (2.5M records per call) are
premium only. Neither is used here.

### Getting a free key

1. Register. Go to [comtrade.un.org](https://comtrade.un.org/), click **Login**
   (top right), then **Sign-up now**. Enter an email, click **Send
   verification code**, paste the code from the "United Nations B2C account
   email verification code" message, click **Verify**, set a password and name,
   then **Create**. You are logged in, and **My Comtrade Free** appears at the
   top right.
2. Open the API portal at
   [comtradeplus.un.org/MyComtrade/APIPortal](https://comtradeplus.un.org/MyComtrade/APIPortal),
   under My Comtrade.
3. Click **Generate API Key** and copy it.

That key is what goes into `comtrade_apikey`. The UN docs note it may be
regenerated periodically, so expect to replace it one day. The API products
portal at [comtradedeveloper.un.org](https://comtradedeveloper.un.org) also
issues keys: subscribe to a product there and copy the *primary key*. Either
one is a `subscription-key` this workflow accepts.

### Going premium

Premium is bought through [shop.un.org](https://shop.un.org), and questions
about a subscription or its licensing go to subscriptions@un.org. The other
route is institutional: many university libraries hold a premium subscription,
and members register with their institutional email address, get approved, and
then request a key. Premium buys the 250,000-record ceiling and the larger daily
quota, nothing else that matters here.

### Which ceiling to put in the config

`records_per_call` in [config/config.yaml](config/config.yaml) has to match the
tier of the key you actually hold:

```yaml
records_per_call: 100000   # free registered key
records_per_call: 250000   # premium, individual or institutional
```

It drives both the packing and the hard failure, and it defaults to the free
value. Setting it above your own ceiling is the one mistake that loses data
quietly: `getFinalData` truncates at the ceiling without raising an error, the
file looks complete, and the guard never fires because it is watching for a
higher number.

### Sources

Checked 2026-10-24.

- [Subscription plans](https://uncomtrade.org/docs/subscriptions/), for the
  per-tier ceilings and quotas.
- [Free and premium subscriptions](https://comtradeplus.un.org/Subscriptions).
- [How to create an account](https://uncomtrade.org/docs/how-to-create-an-account/).
- [Acquiring API subscription keys](https://uncomtrade.org/docs/api-subscription-keys/).
- [API docs](https://comtradeplus.un.org/ApiDocs).
- [Access to UN Comtrade at Yale](https://library.yale.edu/eresources/access/access-un-comtrade),
  an example of the institutional route.

### Setting the variable

Simplest, for one terminal session:

```zsh
export comtrade_apikey="your-key"
snakemake --sdm conda --cores 1
```

**Which file, if you want it to persist.** This is where it usually goes wrong:

| File | Read by | Good for |
|---|---|---|
| `~/.zshrc` | **interactive** shells only | running `snakemake` yourself from a terminal |
| `~/.zshenv` | **every** zsh, interactive or not | scripts, CI, an agent running commands for you |
| a file of your own | nothing, unless `~/.zshrc` sources it | nothing. A silent dead end |

A missing key fails at `comtrade_download` with an explicit message, never a
bare `KeyError`. And it fails there and nowhere earlier, because that rule is
the only one that needs it: the preflight, the mirror and the EU aggregation all
run without. Nothing is wasted when it happens — the plan is already written and
the counting calls were free.

*For MacOS users*: `~/.zshrc` keeps the key in cleartext in a file and in the
environment of every process you run. Better: keep it in the macOS Keychain and
pull it out on demand.

```zsh
# once
security add-generic-password -a "$USER" -s comtrade_apikey -w

# every session, or in a ~/.zshrc function
export comtrade_apikey="$(security find-generic-password -a "$USER" -s comtrade_apikey -w)"
```

On Linux, `pass` with gpg-agent plays the same role; `op read "op://…"` with the
1Password CLI works everywhere.

The API key is then read **inside** `comtrade_download.py`.

### Checking without spending quota

Preflight uses **no key**: it queries Comtrade's public endpoints. So it runs
before you have even subscribed, and it sizes the run:

```zsh
snakemake preflight --sdm conda --cores 1
cat data/comtrade/*.plan.json
```

The plan gives the expected row count, the packing it settled on, and how many
keyed calls the run will cost.

## Layout

```
config/config.yaml           the only file you edit
resources/                   reference tables, versioned (see its README)
workflow/
  Snakefile                  assembly: include order, default target
  rules/
    common.smk               what every source shares: RUN_ID, TARGETS
    comtrade.smk             the Comtrade rules
  scripts/
    comtrade_common.py       logic: resolve, hash, stem, plan, areas
    comtrade_polars.py       logic: guarded sums, EU aggregation
    comtrade_*.py            the rule scripts themselves
```

The division is deliberate, and it is about what can be tested:

| | Holds | Testable |
|---|---|---|
| `.py` | logic | **yes**, it is importable: `_demo()` |
| `.smk` | wiring: paths, rules | no |
| `Snakefile` | assembly | no |

So anything non-trivial goes in a `.py`. Adding a source is a config block, a
`plan()` in its own module, and a rules file — the Snakefile gains one
`include:` line. The contract is written at the top of
[workflow/rules/common.smk](workflow/rules/common.smk).

## Parameters

Everything is in [config/config.yaml](config/config.yaml), commented.

Fixed outside the config, in `FIXED` in
[comtrade_common.py](workflow/scripts/comtrade_common.py): `typeCode=C`
(goods), `breakdownMode=classic`, `customsCode`/`motCode`/`maxRecords`/
`proxy_url` at `None`.

Explicit refusals, all raised at DAG time, before anything is downloaded:

- **`cmd: TOTAL`**: a single reporter-year pair already goes over the cap
  (France 2015, all products: 674,847 rows).
- **`hs_revision: auto`**: needs a correspondence table per revision (the
  `mapping` job). v1 runs in `strict`: codes are passed through as given, with
  a warning on every period where a code returns zero rows.
- **an unknown `classification` or `freq`**, an **unknown country code or
  ISO3**, a `records_per_call` outside its tier. Unchecked, a typo would reach
  the API, come back as a swallowed HTTP error, and surface as a `RuntimeError`
  after five retries.

A key left **empty** in the YAML (`flow:` followed by nothing) means "use the
default", exactly like a missing key. It parses to `None`, and without that
rule `flow:` would have meant *every* flow instead of `M, X`.

### Changing the parameters

**Edit [config/config.yaml](config/config.yaml) and run again.** That is the
normal way to work here, not a workaround.

It is safe, because the parameters are part of the file name. Change the
classification, the codes or the periods, and the fingerprint changes with
them: the new dataset lands **beside** the old one instead of overwriting it,
and both stay readable, dated and comparable.

```zsh
snakemake --sdm conda --cores 1          # HS, codes 4403/4407/4412 -> ac3dd013
# edit classification: H5 in config/config.yaml
snakemake --sdm conda --cores 1          # -> 90d0fcaa, next to the first one

ls data/comtrade/*.parquet | cut -d_ -f4 | sort -u   # the requests you hold
```

So keeping two versions of a dataset is not a matter of naming files carefully:
it is the default behaviour.

#### Why not `--config`

`--config` writes **top-level keys only**. The config is nested, so it cannot
reach inside a source block:

```zsh
snakemake --config aggregate_eu=true            # IGNORED, no warning
snakemake --config comtrade.aggregate_eu=true   # IGNORED too, no warning
```

Both run happily with the old value. The first puts `aggregate_eu` next to
`comtrade:` rather than inside it; the second creates a key literally named
`"comtrade.aggregate_eu"`. Nothing reads either, and nothing says so.

The form that does work passes YAML, and Snakemake merges it into the block
rather than replacing it, so the other keys survive:

```zsh
snakemake --config comtrade='{"aggregate_eu": true}'   # works
```

It is still not the recommended path: it is easy to mistype, it leaves no trace
of what was asked, and the sidecar is then the only record of the real
parameters. Edit the config file instead.

`run_id` is the exception, and the only one. It is read at the **top level** of
the config precisely so that it can be overridden from the command line, which
is what makes step 3 of [Getting started](#getting-started) work.

> The config keeps its nesting on purpose: `comtrade:` grouping its own
> parameters is what makes the file readable, and what will let `worldbank:`
> sit beside it. The answer to the `--config` limitation is to edit the file,
> not to flatten it.

One trap left, about the command line itself: **`--config` swallows every
argument that follows it**, so the target goes *before*.

```zsh
snakemake preflight --cores 1 --config run_id=20261024T120000   # correct
snakemake --config run_id=20261024T120000 preflight             # target eaten
```

## Choosing countries

`reporter` and `partner` accept three forms, mixable in one list:

```yaml
  reporter: all                    # every area, no filter
  reporter: ['251', '156']         # Comtrade numeric codes
  reporter: [FRA, CHN]             # ISO3, any case
  reporter: [FRA, '156']           # mixed is fine
```

ISO3 codes are resolved to numeric codes **before** the request is
fingerprinted, so `[FRA]` and `['250', '251']` are the same request and share
one stem. Writing the readable form costs nothing.

### The limit of ISO3, stated plainly

**15 ISO3 out of 263 point to several Comtrade codes**, because Comtrade keeps
the historical splits as separate areas. All of the matching codes are sent, and
that is what you want:

| ISO3 | Codes | 1996 | 2010 | 2023 |
|---|---|---|---|---|
| `FRA` | 251 France | 385 | 437 | 475 |
| | 250 Metropolitan France | 0 | 0 | 0 |
| `BEL` | 56 Belgium | 0 | 330 | 390 |
| | 58 Belgium-Luxembourg (...1998) | 282 | 0 | 0 |

Row counts for the default wood selection. The codes of one ISO3 are **disjoint
in time**: only one carries data in a given year, so nothing is counted twice,
and sending both is the only way to get Belgium 1996-1998 *and* Belgium
afterwards in one unbroken series. Same for `DEU` (276 / 280 until 1990), `USA`,
`CHE`, `NOR`, `IND`, `SDN`, `VNM`, `YEM`.

That assumption is **checked on the real data**, not trusted: if one ISO3 ever
turns up under two codes in the same period, `comtrade_download` writes a
double-count warning into the log and the sidecar.

Aggregates cannot be selected by ISO3. `EUR` (the European Union, code 97) is
refused, because `exclude_pseudo` drops it anyway and letting it in by name
would be a trap.

### `exclude_pseudo`

On by default. Removes from the delivered file everything that is not a
country:

| Removed | Why |
|---|---|
| `0` World | the sum of every partner; duplicates all bilateral flows |
| `97` European Union, `975` ASEAN | aggregate reporters. Also: no partner code exists for them, and they report nothing before 2000 and 2010 |
| every `..., nes` residual | "not elsewhere specified": a catch-all, not a place |
| `837` Bunkers, `838` Free Zones, `839` Special Categories | not destinations |
| 13 territories with no ISO3 | Kosovo, Channel Islands, Sarawak, and historical territories |

**`490` "Other Asia, nes" is kept, because it is Taiwan.** UNSD cannot name it,
so publishes it under a residual label. It is a single reporting economy, not a
sum of countries: dropping it would delete Taiwan's trade.

Measured weight of the filter on the default selection: `partner=World` alone is
4.3% of the rows in 2023 and 4.8% in 1996; the aggregate reporters are another
5.9% in 2023.

The **full table, with the kind of every area and the reason it is classified
that way**, is in
[resources/comtrade_areas.csv](resources/comtrade_areas.csv) — GitHub renders it
as a sortable table. See [resources/README.md](resources/README.md) for why the
classification is a hand-reviewed list rather than a rule. Refresh it with:

```zsh
python workflow/scripts/build_comtrade_areas.py   # public endpoints, no key
```

The preflight counts stay **unfiltered**, deliberately: the ceiling protects
against a truncated API *response*, which is pre-filter. So the plan's
`expected_rows_total` is an upper bound of the rows delivered, and matches
`rows_received` in the sidecar rather than `rows`.

## Packing and the ceiling

`getFinalData` **truncates without error** at the ceiling of your tier, so the
packing is not a guessed constant, it is measured against `records_per_call`:

1. a **free** count per period through the public endpoint. The count that
   comes back is the real one, not capped;
2. greedy packing of consecutive periods under 80% of the ceiling (margin for
   upstream revisions between preflight and download);
3. if a single period goes over on its own, bisection on the codes, still on
   free counts;
4. hard failure at the ceiling, warning from 80% on.

Counts are additive. A request over P1..Pn returns the union of the rows, so
the sum of the per-period counts *is* the pack's count. That is what makes it
possible to plan optimally without downloading a row.

Measured over 1996-2023, codes 4403/4407/4412, all reporters, M+X: 539,660
rows and 28 free counting calls. Replaying those counts through the packer
gives **9 keyed calls** on a free key (packing under 80,000) and **3** on
premium (packing under 200,000).

## Outputs

```
data/comtrade/
  comtrade_HS_A_ac3dd013_20261024T120000.parquet         # canonical artifact
  comtrade_HS_A_ac3dd013_20261024T120000.csv             # for colleagues
  comtrade_HS_A_ac3dd013_20261024T120000.meta.json       # sidecar
  comtrade_HS_A_ac3dd013_20261024T120000.mirror.parquet  # if mirror: true
  comtrade_HS_A_ac3dd013_20261024T120000.mirror.csv
  comtrade_HS_A_ac3dd013_20261024T120000.eu.parquet      # if aggregate_eu: true
  comtrade_HS_A_ac3dd013_20261024T120000.eu.csv
  latest_ac3dd013.parquet -> ...                         # link by hash
```

`ac3dd013` is a truncated SHA-256 fingerprint of the **resolved** parameter
set, sorted and normalized. It answers the one question a timestamp does not
settle: are two files the same run at two dates, and therefore comparable, or
two different requests?

What is left out of it is a deliberate line: a key is excluded when it changes
the *delivery* but not the *request*. `format`, `csv_sep`, `run_id`,
`records_per_call` (a transport ceiling: the same selection on a free and a
premium key is the same data), and `mirror` and `aggregate_eu`, which add a
derived file next to the canonical one without touching it.

`exclude_pseudo` **is** in the fingerprint, because it changes the rows
delivered. Without that, two files sharing a stem could hold different data.

```zsh
ls data/comtrade/*.parquet | cut -d_ -f4 | sort -u   # distinct requests
ls data/comtrade/*ac3dd013*                          # every run of one of them
```

The `latest_` link is indexed by hash. A global `latest` becomes wrong as soon
as there are two requests in the folder.

### Sidecar

One per data file. The resolved parameters on one side, the upstream state of
the data on the other: `classificationCode`, `totalRecords`,
`datasetChecksum` and `lastReleased` per (reporter, period).

That second half is the most useful, because **Comtrade rewrites history**.
For instance, French trade in 2020 was published on 2021-05-26, then revised on
2026-05-23. Without this record, two runs of the same parameters that diverge
are inexplicable, and one may start suspecting his own code. `datasetChecksum`
is the strongest anchor: unchanged means identical data.

The API key never appears in it.

## Reading the CSV

The workflow produces the data in two file formats: csv and parquet. Using the
CSV file instead of the parquet one has two costs.

**Size**: roughly 11x the parquet.

**Lost types**: `"01"` read back becomes `1`.

When reading the CSV file on, for instance, R or Stata, beware to use:

```r
read.csv("file.csv", colClasses = c(cmdCode = "character"))
```

```stata
import delimited "file.csv", stringcols(_all)
```

Country names containing a comma (`China, Hong Kong SAR`) are quoted
automatically by polars and survive the comma separator. No exotic separator
needed.

## Mirror flows

A bilateral flow is reported twice: by the exporter (`X`) and by the importer
(`M`). The mirror file puts them side by side, suffixed `_exp` and `_imp`, one
row per directed edge `(period, cmdCode, exporterISO, importerISO)`.

Two non-negotiable rules apply:

- **No reconciliation.** No FOB/CIF averaging, no substituting one side for the
  other. The gap between the two declarations is a data quality signal.
- **Nulls stay null, not zero.** Sums are guarded: a group that is entirely
  null stays null. Otherwise a non-reporter turns into a reporter declaring
  zero and the diagnostic disappears.

## The EU as one node

`aggregate_eu: true` adds `{stem}.eu.{parquet,csv}`, where the member states
are summed into a single `EU` node and **intra-EU trade is deleted**: once both
ends of a flow are `EU`, the flow is internal to the node and summing it would
make the aggregate trade with itself. Same two rules as the mirror, guarded
sums included.

Built **from the member states, never from Comtrade's code 97**. That code is
unusable here, and measurably so:

- it exists **only as a reporter**. There is no `European Union` partner code,
  so a selection on 97 would give the EU's own declarations and nothing
  declared towards it.
- it reports **nothing before 2000** on the wood codes (ASEAN, nothing before
  2010), so a 1996-2023 request would carry four silently empty years.

Summing the members has neither problem: it works on rows already downloaded,
on both sides at once, over the whole period.

Membership is **dated**, in
[resources/eu_membership.csv](resources/eu_membership.csv): a country is folded
in only for the years it was a member, so the United Kingdom is inside the EU
through 2020 and a separate reporter from 2021, and Croatia joins in 2013.
Without that, a flat list of today's 27 would double count the UK for 25 years,
since the EU aggregate of those years already contains it. The file is keyed on
ISO3, so `BEL` picks up both code 56 and code 58 (Belgium-Luxembourg, the
1996-1998 declarant) with nothing typed by hand.

A **derived file**: the canonical parquet is untouched, so the aggregation can
be redone, or the Brexit cut-off changed, without downloading anything again.
It is also a **reduced** view — the per-declaration columns (`isReported`,
`legacyEstimationFlag`, the estimation flags, customs and transport modes) are
dropped, because they describe one declaration and mean nothing once several
are summed. The log names every column it drops. Quantity units stay grouping
keys, so quantities declared in different units are never added together.

Needs `include_desc: true`: the aggregation keys on the ISO columns, and
refuses explicitly without them.

## `comtradeapicall` traps

Checked on the installed 1.2.1 and on the `main` source (1.3.2, pinned here).

**HTTP errors are swallowed.** `getPreviewData` prints the response body and
returns `None` without raising, so the `except:` in `_getFinalData` never fires
on a 429 or a 503. And `pandas.concat` **ignores a `None` without error**
(checked on pandas 2.2.2 and 3.0.1): a failed period would vanish from the
result without leaving a trace. Hence `call()` in `comtrade_common.py`, which
treats a `None` return as a failure and raises once the attempts are exhausted.
Every call goes through it.

**Preview accepts one period per call**:
`{"error":"Maximum number of periods for preview is 1"}`. No consequence, since
it is free.

**Async is not implemented here, and that is deliberate.** It would raise the
cap to 2.5M, but `downloadAsyncFinalDataRequest` loops with no timeout and no
maximum number of attempts, lets the server pick the file name (so no `output:`
can be declared), and crashes with a `TypeError` if the submission fails.

## Check

No test suite. Two runnable self-checks instead, both free of the API, of conda
and of the network, so they still work when the environment does not:

```zsh
python workflow/scripts/comtrade_common.py   # hash, packing, areas, refusals
python workflow/scripts/comtrade_polars.py   # EU aggregation, download checks
```

The first verifies that the hash ignores list order, case and types but not a
real change of request; that `period.stop` is inclusive; that no period is lost
during packing; that `[FRA]` and `['250','251']` are one request; that `BEL`
resolves to both of its codes; that an empty YAML key falls back to the default
while an explicit `false` survives; and that `TOTAL`, `hs_revision: auto`, an
unknown classification, frequency, country code or ISO3 are all refused.

The second checks the EU aggregation on a dataframe written by hand: nulls stay
null, intra-EU trade is deleted, the UK is in the EU in 2019 and out of it in
2021, Croatia is out in 2012 and in from 2013, Belgium's historical code is
folded in, quantities in different units are not added up, and non-members are
left alone. It then shows each download check a deliberately broken dataset —
a duplicated declaration, a missing column, a foreign product code, a period
nobody asked for, a falsified count, an EU sum that does not add up — and
checks that it is caught.

```zsh
python workflow/scripts/build_comtrade_areas.py   # refresh the areas table
```

### Checks on the downloaded data

The self-checks above run on dataframes written by hand. These run on the real
response, on every download, and they exist for a failure the self-checks
cannot see: a file that is **plausible but wrong**. It opens, the types are
right, the row count looks reasonable, and nobody notices until it is
published.

They run **before the pseudo-country filter and before anything is written**.
So a dataset that fails leaves no file at all: Snakemake deletes the outputs of
a failed job but keeps its log, which holds the diagnosis.

Eleven checks, in `workflow/scripts/comtrade_polars.py` so that each one can be
shown a deliberately broken dataset and proved to detect it. A check that has
only ever seen a good file proves nothing.

| Check | What it refuses |
|---|---|
| `schema` | a column the rest of the pipeline reads is missing |
| `counts_period` | a period holds fewer rows than the free preflight counted |
| `counts_total` | the total differs from `expected_rows_total` |
| `classification` | something other than the classification asked for |
| `frequency` | another frequency, or services instead of goods |
| `flow` | a flow direction nobody asked for |
| `cmd` | a product code nobody asked for |
| `periods_extra` | a period nobody asked for |
| `periods_missing` | a period asked for and absent |
| `iso3_unique` | one ISO3 under two reporter codes in one period |
| `duplicates` | the same declaration received twice |

Plus one per derived file: the mirror and the EU aggregate must **conserve
value**. The EU file in particular states its arithmetic out loud — raw total
minus the intra-EU trade it deleted — and refuses to be written if the two
sides differ.

Each result lands in the sidecar under `checks`, so a run can be audited
without reading the log.

**`fail` versus `warn`.** A `fail` stops the run: the file would answer a
different question from the one asked. A `warn` is recorded and the run
continues, and two of them are warnings on purpose:

- **the counts.** Comtrade rewrites history, so a gap between the preflight and
  the download can be a perfectly legitimate revision rather than a bug.
  Telling the two apart needs re-reading `datasetChecksum` after the download,
  which is not done yet. Blocking a workflow on a revision would be worse than
  warning about it. When a count warning appears, the sidecar's
  `datasetChecksum` is what settles it.
- **a forced classification.** Asking `HS` and getting a mix of `H0`, `H1` and
  `H2` is normal — `HS` means "the revision in force that year", and each
  declarant has its own. Asking `H5` and getting something else may or may not
  be normal: whether UNSD labels a converted row with the target revision or
  the source one has not been measured, and the public preview cannot settle it.
  So it warns rather than refusing a run that might be correct.

## Open jobs

- **`mapping`**: `hs_revision: auto` and the HS to FAO conversion. Needs a
  correspondence table per revision. The FAO table exists in
  `trade_network_analysis` (`resources/inhouse/correspondence_FAO_HS.json`);
  the general case needs the UNSD H0 to H6 correlation tables.
- **async**: to open only if a legitimate selection goes over 250K. Means
  rewriting submit / check / download by hand.
- **other sources**: World Bank, FAOSTAT. Same contract: free preflight, dated
  file, sidecar.
- **tell a Comtrade revision from a bug**: what is left of the download
  checks, which otherwise landed (see Check above). The counts can only
  **warn** today, because a gap between the preflight and the download is a
  legitimate revision as often as it is a lost pack. Re-reading
  `datasetChecksum` after the download would separate the two: checksum moved
  means revision, checksum unchanged with a different count means a bug here,
  and the warning could become a refusal. It costs a few free calls. One
  caution for whoever picks it up: `totalRecords` from the availability
  endpoint counts the whole reporter-period dataset, not our commodity
  selection, so it is not a valid baseline — the preview counts are.
- **compare with `comtradr`**:
  [comtradr](https://docs.ropensci.org/comtradr/index.html) (rOpenSci) wraps
  the same API in R, with `ct_get_data()`, `ct_commodity_lookup()`,
  `ct_get_ref_table()`, `ct_get_bulk()` for premium bulk files, and a key read
  from `set_primary_comtrade_key()` or `COMTRADE_PRIMARY`. Its docs state a
  limit this workflow knows nothing about: "You can request a maximum interval
  of twelve years from the API." If that applies to `data/v1/get`, a pack also
  has a length ceiling, independent of the row ceiling, and `pack_periods()`
  should cap at twelve periods. Unverified here, and worth settling first,
  since a low-volume selection would happily pack twenty years into one call.
  Its "Querying large amounts of data" and caching articles are the other
  reason to read it.
