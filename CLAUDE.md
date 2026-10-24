# CLAUDE.md

Guidance for Claude Code in `data-fetch`. **Read [README.md](README.md) first**:
it says what the repo does and why. This file says how to work in it.

## Actual state (2026-10-24, commit cc3ead8)

| Rule | State |
|---|---|
| `comtrade_preflight` | **verified end to end** against the real API |
| `comtrade_download` | **never run**, requires an API key |
| `comtrade_mirror` | **never run**, depends on the output of `download` |

Syntax validated and DAG valid for all three, but that is not the same as
running them. The first real download is the user's to make. **Never claim that
`download` or `mirror` works**: say that they have not been run.

## Commands

```zsh
python workflow/scripts/comtrade_common.py        # the only runnable check
snakemake preflight --sdm conda --cores 1         # free, no key
snakemake --sdm conda --cores 1                   # full download (premium quota)
snakemake -n                                      # dry run, no conda needed
snakemake --sdm conda --cores 1 --config run_id=20261024T120000   # replays a run
```

**No test suite.** Verification means running the rule, reading
`workflow/logs/<rule>_<stem>.log`, and inspecting `data/comtrade/`.

`comtrade_common.py` has a `_demo()` under `__main__`: stable hash, packing,
refusal of `TOTAL` and of `hs_revision: auto`. Any non-trivial logic added
there leaves an `assert` behind.

## Architecture

```
Snakefile ──(import at DAG time)──> comtrade_common.resolve_params, params_hash, stem
    │                                └─ sets STEM, hence every output path
    └─ include rules/comtrade.smk
         comtrade_preflight  → {STEM}.plan.json          PUBLIC endpoints, off quota
         comtrade_download   → {STEM}.{parquet,csv} + {STEM}.meta.json   keyed quota
         comtrade_mirror     → {STEM}.mirror.{parquet,csv}
```

`comtrade_common.py` is shared between the Snakefile and the scripts. It holds
the ceilings (`caps()`, `FREE_RECORDS_PER_CALL`, `PREMIUM_RECORDS_PER_CALL`,
`WARN_FRACTION`), the `FIXED` constants, parameter resolution
and hashing, the `call()` wrapper, `selection()`, `pack_periods()` and
`bisect_codes()`.

## Gotchas

All found by running things, not by rereading them.

- **Never `from __future__ import annotations` in a `script:` file.**
  Snakemake inserts a preamble before the file's content, so the import is no
  longer on the first line and you get a `SyntaxError`. The env is Python 3.12:
  `str | None` and `list[str]` work natively. `comtrade_common.py` is an
  imported module, not a `script:`, so it can keep it.
- **`--config` swallows every argument that follows.** The target goes
  **before**: `snakemake preflight --cores 1 --config run_id=…`, never the
  other way round.
- **`_getFinalDataAvailability(typeCode, freqCode, clCode, period, reporterCode)`**:
  `typeCode` comes first. Forgetting it only raises at runtime.
- **Output paths depend on `params_hash`, computed at DAG time.** Touching
  `resolve_params()` or `_NOT_IN_HASH` renames **every** future file and
  detaches past runs from their request. That is not a free refactor.
- **`comtrade_common.py` must stay cheap to import**: the Snakefile imports it
  before any rule. No polars, no comtradeapicall in there.
- **The row ceiling is the key's, not the package's.** `records_per_call` in
  the config is the Data API ceiling of the subscription tier: 100K on a free
  key, 250K on premium. `caps()` derives the warning threshold from it at 80%.
  It defaults to the free value and is in `_NOT_IN_HASH`, because the same
  selection pulled on either key is the same data. Setting it above the real
  ceiling truncates the response with no error and the guard never fires.
- **`comtradeapicall` returns pandas.** Convert with `pl.from_pandas()` at the
  boundary, in the scripts; everything else is polars.
- **`period.stop` is INCLUSIVE.** A deliberate correction of the `stop - 2` in
  `trade_network_analysis`. Do not "harmonize" it back to the old behavior.
- **`parquet` is forced into `FORMATS`** even if the config does not ask for
  it: `comtrade_mirror` reads the parquet, never the CSV.
- **The `latest_<hash>` link is written by `comtrade_download.py`**, not by a
  rule. Indexed by hash, because a global `latest` becomes wrong as soon as
  there are two requests in the folder.

## `comtradeapicall` traps

Checked on the 1.2.1 installed locally and on the `main` source (1.3.2,
pinned).

- **`getFinalData` truncates silently at the tier ceiling** (100K free, 250K
  premium). No error at the ceiling. Hence the hard failure in `fetch_pack()`,
  against `records_per_call`.
- **HTTP errors are swallowed**: `getPreviewData` prints the body and returns
  `None` without raising. So the `except:` in `_getFinalData` never fires on a
  429, and `pandas.concat` **ignores a `None` without error** (checked on
  pandas 2.2.2 and 3.0.1): a failed period would disappear from the result
  without a trace. **Every call goes through `call()`**, which treats `None` as
  a failure. Never call the package directly.
- **The public preview accepts one period per call**:
  `{"error":"Maximum number of periods for preview is 1"}`. Hence one counting
  call per period. Free, so it does not matter.
- **The public count returns the real count, not a capped one.** The "limited
  to 500 records" in the README applies only to data rows.
- **Async not implemented, deliberately.** The cap is 2.5M, but
  `downloadAsyncFinalDataRequest` loops with no timeout and no maximum number
  of attempts, lets the server name the file (so no `output:` can be
  declared), and crashes with a `TypeError` if the submission fails. Opening it
  up means rewriting submit / check / download by hand.

## Security

- The key is read **in `comtrade_download.py`**, via `os.environ`. **Never in
  `params:`**: Snakemake records every param in cleartext in
  `.snakemake/metadata/`. That is exactly the leak found in
  `trade_network_analysis`.
- **No `envvars:` directive** in the Snakefile, on purpose: it would make the
  whole workflow fail without a key, preflight included.
- **`comtrade_preflight` is the only rule an agent can run without risk**:
  public endpoints, no key, no quota. `comtrade_download` spends the key's
  daily quota (500 calls/day free, paid on premium), so do not run it without
  an explicit request.
- The key never appears in the sidecar, the logs, or an error message. If some
  code logs a request URL, mask it: `subscription-key` travels in the query
  string.

## Measured reference numbers

To spot an abnormal run.

```
Default config (1996-2023, codes 4403/4407/4412, M+X, all reporters):
  539,660 rows expected,  28 free counting calls
  vintage: 4,756 records (reporter x period)
  packs, free key (records_per_call 100000, packing under 80,000): 9 calls
    1996-2000 → 75,120 | 2001-2004 → 72,785 | 2005-2007 → 59,893
    2008-2010 → 60,867 | 2011-2013 → 61,208 | 2014-2016 → 62,894
    2017-2019 → 64,248 | 2020-2022 → 62,004 | 2023      → 20,641
  packs, premium (records_per_call 250000, packing under 200,000): 3 calls
    1996-2006 → 187,296 | 2007-2015 → 184,257 | 2016-2023 → 168,107

Orders of magnitude, all reporters, M+X:
  France 2015, ALL products  → 674,847   (hence the refusal of cmd: TOTAL)
  23 wood codes, 1996         →  33,092
  56 wood codes, 2017         →  95,289
  67 wood codes, 2023         → 101,252
```

Comtrade rewrites history: France 2020 published on 2021-05-26, revised on
2026-05-23. Two identical runs can therefore legitimately differ, and the
sidecar's `datasetChecksum` settles it.

## Conventions

- Config commented in [config/config.yaml](config/config.yaml). A parameter
  that never varies goes in `FIXED`, not in the config.
- Comments and docs in English; commit messages in English.
- Mirror flows: **never reconciled**, sums guarded so that nulls stay null. The
  gap between declarations is a signal, not noise.
- Explicit refusals rather than obscure failures later: `cmd: TOTAL` and
  `hs_revision: auto` raise in `resolve_params()`.

## Open jobs

- **`mapping`**: `hs_revision: auto` and HS to FAO. Needs a correspondence
  table per revision. In `strict`, a code absent from a revision returns zero
  rows and produces a warning, nothing more.
- **download checks**: the plan's per-period counts are never compared to the
  rows received, nor the total to `expected_rows_total`. Only the ceiling,
  empty packs and absent periods are caught today. The availability
  `totalRecords` is not a usable baseline (whole reporter-period, not our
  codes); the preview counts are. See README for the full note.
- **compare with `comtradr`**: the R wrapper over the same API
  (https://docs.ropensci.org/comtradr/). Its docs state a maximum interval of
  twelve years per request. If that holds for `data/v1/get`, `pack_periods()`
  needs a length cap as well as a row cap. Unverified.
- **async**: to open only if a legitimate selection goes over 250K.
- **other sources**: World Bank, FAOSTAT. Same contract: free preflight, dated
  file, sidecar.

## Relation to `trade_network_analysis`

Neighboring repo (`../trade_network_analysis`), the source of this code. **Left
there on purpose**, do not copy it over:

- `process_param()`: splitting by HS revision; it serves the `mapping` job, not
  the `strict` v1.
- `correspondence_FAO_HS.{py,json}`: the FAO/HS table; same reason.

Taken over: the mirror join from `network_objects.py`, the conda env (1.2.1 to
1.3.2), the ignore patterns. `get_uncomtrade_data.py` only served as a
reference for the shape of the call.

**Do not modify `trade_network_analysis`**: the manuscript is submitted, its
`comtradeapicall ==1.2.1` pin and its paths stay frozen.
