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

Downloading requires a **premium** UN Comtrade subscription key, because it
goes through `getFinalData` and not the public endpoint. Get one at
[comtradedeveloper.un.org](https://comtradedeveloper.un.org): create an
account, subscribe to the *comtrade-v1* product, then copy the *primary key*.

The workflow reads it from the environment variable **`comtrade_apikey`**, and
nowhere else. It appears in no configuration file.

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
premium calls the run will cost.

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
`comtrade_download` spends paid quota.

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

## Packing and the cap

`getFinalData` caps at **250,000 records** and **truncates without error** at
the cap. So the packing is not a guessed constant, it is measured:

1. a **free** count per period through the public endpoint. The count that
   comes back is the real one, not capped;
2. greedy packing of consecutive periods under **200,000** rows (margin for
   upstream revisions between preflight and download);
3. if a single period goes over on its own, bisection on the codes, still on
   free counts;
4. hard failure at **250,000** rows received, warning from 200,000 on.

Counts are additive. A request over P1..Pn returns the union of the rows, so
the sum of the per-period counts *is* the pack's count. That is what makes it
possible to plan optimally without downloading a row.

Measured over 1996-2023, codes 4403/4407/4412: **539,660 rows, 3 premium
calls** (plus 28 free counting calls).

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
