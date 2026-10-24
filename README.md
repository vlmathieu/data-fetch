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

Two caveats on that table. The per-day figures come from the preview-API quota
row of the subscription plans page plus its sentence that a free subscription
includes "data API access for up to 500 calls/day"; the page states no separate
Data API quota per tier. And it lists an API rate limit of 1 to 5 calls per
second without mapping those values to tiers, which is part of why every call
goes through `call()` with backoff.

Bulk files and the async endpoint (2.5M records per call) are premium only.
Neither is used here.

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
then request a key (Yale, for one, documents that path). Premium buys the
250,000-record ceiling and the larger daily quota, nothing else that matters
here.

### Which ceiling to put in the config

`records_per_call` in [config/config.yaml](config/config.yaml) has to match the
tier of the key you actually hold:

```yaml
records_per_call: 100000   # free registered key
records_per_call: 250000   # premium, individual or institutional
```

It drives both the packing and the hard failure, and it defaults to the free
value. Setting it above your own ceiling is the one mistake that loses data
quietly: `getFinalData` truncates at the ceiling without raising, the file
looks complete, and the guard never fires because it is watching for a higher
number.

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

To avoid retyping it every session, zsh will read it from `~/.zshrc`, but it is
then in cleartext in a file and present in the environment of every process you
run. Better: keep it in the macOS Keychain and pull it out on demand.

```zsh
# once
security add-generic-password -a "$USER" -s comtrade_apikey -w

# every session, or in a ~/.zshrc function
export comtrade_apikey="$(security find-generic-password -a "$USER" -s comtrade_apikey -w)"
```

On Linux, `pass` with gpg-agent plays the same role; `op read "op://…"` with the
1Password CLI works everywhere.

### Checking without spending quota

Preflight uses **no key**: it queries Comtrade's public endpoints. So it runs
before you have even subscribed, and it sizes the run:

```zsh
snakemake preflight --sdm conda --cores 1
cat data/comtrade/*.plan.json
```

The plan gives the expected row count, the packing it settled on, and how many
keyed calls the run will cost.

### What the key must never touch

- It is read **inside** `comtrade_download.py`, never declared in a Snakemake
  rule's `params:`.
- It is not written to the sidecar, to the logs, or into an error message. If
  you add code that logs a request URL, mask it: `subscription-key` travels in
  the query string.
- `data/` and `.snakemake/` have been in `.gitignore` **and** `.dockerignore`
  since the first commit.

One limit worth knowing: no file or environment variable mechanism hides a
secret from a process running under your own account. What actually protects it
is a vault you unlock interactively (Keychain, 1Password, `pass`), plus the
simple rule that `comtrade_preflight` is the only rule safe to run.
`comtrade_download` spends your key's daily quota, which is a paid quota on
premium.

## Parameters

Everything is in [config/config.yaml](config/config.yaml), commented.

Fixed outside the config, in `FIXED` in
[comtrade_common.py](workflow/scripts/comtrade_common.py): `typeCode=C`
(goods), `breakdownMode=classic`, `customsCode`/`motCode`/`maxRecords`/
`proxy_url` at `None`. A parameter that never varies is a parameter to delete.

Two explicit refusals:

- **`cmd: TOTAL`**: a single reporter-year pair already goes over the cap
  (France 2015, all products: 674,847 rows).
- **`hs_revision: auto`**: needs a correspondence table per revision (the
  `mapping` job). v1 runs in `strict`: codes are passed through as given, with
  a warning on every period where a code returns zero rows.

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
  comtrade_HS_A_8dbfc6dd_20261024T120000.parquet         # canonical artifact
  comtrade_HS_A_8dbfc6dd_20261024T120000.csv             # for colleagues
  comtrade_HS_A_8dbfc6dd_20261024T120000.meta.json       # sidecar
  comtrade_HS_A_8dbfc6dd_20261024T120000.mirror.parquet  # if mirror: true
  comtrade_HS_A_8dbfc6dd_20261024T120000.mirror.csv
  latest_8dbfc6dd.parquet -> ...                         # link by hash
```

`8dbfc6dd` is a truncated SHA-256 fingerprint of the **resolved** parameter
set, sorted and normalized, excluding the delivery keys (`format`, `csv_sep`,
`mirror`, `run_id`). It answers the one question a timestamp does not settle:
are two files the same run at two dates, and therefore comparable, or two
different requests?

```zsh
ls data/comtrade/*.parquet | cut -d_ -f4 | sort -u   # distinct requests
ls data/comtrade/*8dbfc6dd*                          # every run of one of them
```

The `latest_` link is indexed by hash. A global `latest` becomes wrong as soon
as there are two requests in the folder.

### Sidecar

One per data file. The resolved parameters on one side, the upstream state of
the data on the other: `classificationCode`, `totalRecords`,
`datasetChecksum` and `lastReleased` per (reporter, period).

That second half is the least obvious and the most useful, because **Comtrade
rewrites history**. France 2020 was published on 2021-05-26, then revised on
2026-05-23. Without this record, two runs of the same parameters that diverge
are inexplicable, and you start suspecting your own code. `datasetChecksum` is
the strongest anchor: unchanged means identical data.

The API key never appears in it.

## Reading the CSV (Stata, R)

CSV is the only format that both Stata and R read without installing a
package. It has two costs.

**Size**: roughly 11x the parquet.

**Lost types**: `"01"` read back becomes `1`. The pipeline never reads the CSV,
since parquet is canonical, but your colleagues will:

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

A bilateral flow is declared twice: by the exporter (`X`) and by the importer
(`M`). The mirror file puts them side by side, suffixed `_exp` and `_imp`, one
row per directed edge `(period, cmdCode, exporterISO, importerISO)`.

Two non-negotiable rules:

- **No reconciliation.** No FOB/CIF averaging, no substituting one side for the
  other. The gap between the two declarations is a data quality signal.
- **Nulls stay null, not zero.** Sums are guarded: a group that is entirely
  null stays null. Otherwise a non-reporter turns into a reporter declaring
  zero and the diagnostic disappears.

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

```zsh
python workflow/scripts/comtrade_common.py   # stable hash, packing, refusals
```

Verifies that the hash ignores list order and types but not a real change of
request, that `period.stop` is inclusive, that no period is lost during
packing, and that `TOTAL` and `hs_revision: auto` are both refused.

## Open jobs

- **`mapping`**: `hs_revision: auto` and the HS to FAO conversion. Needs a
  correspondence table per revision. The FAO table exists in
  `trade_network_analysis` (`resources/inhouse/correspondence_FAO_HS.json`);
  the general case needs the UNSD H0 to H6 correlation tables.
- **async**: to open only if a legitimate selection goes over 250K. Means
  rewriting submit / check / download by hand.
- **other sources**: World Bank, FAOSTAT. Same contract: free preflight, dated
  file, sidecar.
