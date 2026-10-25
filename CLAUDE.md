# CLAUDE.md

Guidance for Claude Code in `data-fetch`. **Read [README.md](README.md) first**:
it says what the repo does and why. This file says how to work in it.

## Actual state (2026-10-25)

| Rule | State |
|---|---|
| `comtrade_preflight` | **verified end to end** against the real API |
| `comtrade_download` | **verified end to end** on 2026-10-25, free key, 1 call |
| `comtrade_mirror` | **verified end to end** on the same run |
| `comtrade_eu` | **never run**: needs `aggregate_eu: true`, only self-checked |

The first real download happened on 2026-10-25: 2000-2003, code 4403, M+X, all
reporters. It found a bug the self-checks could not (see `partner2Code` in the
gotchas). **`comtrade_eu` has still never run**: say that it has not, rather
than that it works.

## Commands

```zsh
python workflow/scripts/comtrade_common.py        # hash, packing, areas, refusals
python workflow/scripts/comtrade_polars.py        # EU aggregation
python workflow/scripts/build_comtrade_areas.py   # refresh resources/, public, no key
snakemake preflight --sdm conda --cores 1         # free, no key
snakemake --sdm conda --cores 1                   # full download (premium quota)
snakemake -n                                      # dry run, no conda needed
snakemake --sdm conda --cores 1 --config run_id=20261024T120000   # replays a run
```

**No test suite.** The two `_demo()` functions under `__main__` are the check.
Beyond them, verification means running the rule, reading
`workflow/logs/<rule>_<stem>.log`, and inspecting `data/comtrade/`.

Any non-trivial logic added to either module leaves an `assert` behind.

## Architecture

```
Snakefile                  assembly only: include order, `rule all`
    ├─ include rules/common.smk    sys.path, RUN_ID, TARGETS = []
    └─ include rules/comtrade.smk
         CT = plan(config["comtrade"], RUN_ID)   ← AT DAG TIME
         TARGETS += CT["targets"]                  sets STEM, hence every path
         preflight           short target, no output
         comtrade_preflight  → {STEM}.plan.json          PUBLIC endpoints, off quota
         comtrade_download   → {STEM}.{parquet,csv} + {STEM}.meta.json   keyed quota
         comtrade_mirror     → {STEM}.mirror.{parquet,csv}
         comtrade_eu         → {STEM}.eu.{parquet,csv}   if aggregate_eu

resources/                 versioned, NOT fetched at runtime
  comtrade_areas.{json,csv}   312 areas: code, iso3, desc, kind  (generated)
  eu_membership.csv           dated EU membership, keyed on ISO3  (by hand)
```

**The split is about what can be tested**, and it decides where new code goes:

| | Holds | Testable |
|---|---|---|
| `.py` | logic | **yes**, importable: `_demo()` |
| `.smk` | wiring: paths, rules | no |
| `Snakefile` | assembly | no |

So anything non-trivial belongs in a `.py`. `plan()` exists for exactly that
reason: the parquet forcing and the target list used to live in the Snakefile,
where no assert could reach them.

Adding a source: a config block, a `plan()` in its own module, a
`rules/<source>.smk` doing `TARGETS += ...`, and one `include:` line. The
contract is at the top of `rules/common.smk`.

**Two shared modules, and the split matters:**

- `comtrade_common.py` is imported **by the Snakefile**, before any rule. It
  must stay free of polars, pandas and network access. It holds the ceilings
  (`caps()`, `FREE_RECORDS_PER_CALL`, `PREMIUM_RECORDS_PER_CALL`,
  `WARN_FRACTION`), the `FIXED` constants, parameter resolution and hashing,
  the `call()` wrapper, `selection()`, `pack_periods()`, `bisect_codes()`, and
  the areas table (`_load_areas()`, `resolve_areas()`, `pseudo_codes()`, stdlib
  `json` only).
- `comtrade_polars.py` is imported **by the scripts only**, inside the conda
  env, so polars is allowed. It holds `guarded_sum()` (shared with the mirror)
  and the EU aggregation (`eu_membership()`, `aggregate_eu()`).

`aggregate_eu.py` is only the Snakemake wiring: the logic lives in
`comtrade_polars.py` because a `script:` file cannot be run on its own, and
therefore cannot carry a self-check.

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
- **`--config` only writes TOP-LEVEL keys, and the config is nested.** This is
  the more expensive of the two traps, because it is silent.
  `--config aggregate_eu=true` puts the key *next to* `comtrade:` rather than
  inside it, and `--config comtrade.aggregate_eu=true` creates a key literally
  named `"comtrade.aggregate_eu"`. Both run with the old value and say nothing.
  The working form passes YAML, and Snakemake **merges** it into the block
  instead of replacing it, so the other keys survive (verified):
  `--config comtrade='{"aggregate_eu": true}'`.
  **The intended way to change a parameter is to edit `config/config.yaml` and
  run again**: the fingerprint changes, so the new dataset lands beside the old
  one. Do not propose flattening the config to work around this — its nesting
  is what makes it readable and what will let `worldbank:` sit beside
  `comtrade:`. `run_id` is read at the top level on purpose, and is the one
  override meant for the command line.
- **A rule's f-strings are evaluated when the rule is PARSED**, not at job
  time. That is what makes the short names (`STEM`, `OUT`, `FORMATS`) safe when
  several sources are included: a later `include` reassigning them cannot
  affect rules already declared. Do not turn them into job-time lambdas
  without rethinking that.
- **`rule all` sits after the includes and needs `default_target: True`.**
  Without the directive Snakemake takes the first rule it parsed as the
  default, which would be `preflight`. Moving `rule all` to the top is not the
  fix: `TARGETS` is still empty there.
- **`_getFinalDataAvailability(typeCode, freqCode, clCode, period, reporterCode)`**:
  `typeCode` comes first. Forgetting it only raises at runtime.
- **Output paths depend on `params_hash`, computed at DAG time.** Touching
  `resolve_params()` or `_NOT_IN_HASH` renames **every** future file and
  detaches past runs from their request. That is not a free refactor. It was
  done once, on 2026-10-25, when `exclude_pseudo` entered the hash:
  `8dbfc6dd` → `ac3dd013` on the default config. It was nearly free then,
  because the only artifact on disk was a plan.json a free preflight
  regenerates. It will not be that cheap again.
- **What goes in the hash is a line, not a habit.** A key enters it when it
  changes the rows delivered (`exclude_pseudo`), and stays out when it only
  adds a derived file next to the canonical one (`mirror`, `aggregate_eu`) or
  describes transport (`format`, `csv_sep`, `records_per_call`, `run_id`).
- **`reporter`/`partner` accept ISO3 or numeric codes**, resolved to codes
  **before** hashing, so `[FRA]` and `['250','251']` share one stem. An ISO3
  resolves to **every** matching code: 15 ISO3 cover several Comtrade areas
  (historical splits), and they are disjoint in time, so sending all of them is
  both safe and the only way to get an unbroken series (`BEL` needs code 58 for
  1996-1998 and code 56 after). Measured, not assumed — and the download warns
  if one ISO3 ever shows up under two codes in one period.
- **`resources/comtrade_areas.json` is versioned on purpose.** `resolve_params`
  runs at DAG time: a network call there would make `snakemake -n` fail on a
  connection blip, and a fingerprint must not depend on a file UNSD can
  rewrite. Refresh it by hand with `build_comtrade_areas.py`, which **refuses
  to run** if UNSD added an area it cannot classify.
- **`kind` in the areas table is a hand-reviewed list, not a rule.** Every
  mechanical test fails both ways: "no valid ISO3" misses the European Union
  (97/`EUR`), "Europe EU, nes" (492/`MCO`) and the Neutral Zone (536/`NTZ`),
  and catches real places that merely lack an ISO3 (Kosovo, Channel Islands).
  UNSD's own `isGroup` marks 4 areas out of 312. Do not "simplify" it to a
  regex: the previous repo's `excluded_iso: ['XX','_X','\d']` missed the EU.
- **`partner2Code` is `0` on EVERY row when partner2 is not requested**, and
  `0` is the code of "World". It is a "not applicable" placeholder, not an
  aggregate. Filtering `exclude_pseudo` on it dropped 16,934 rows out of
  16,934 on the first real download. Hence `PSEUDO_SIDES` in
  `comtrade_polars.py`, which lists `reporterCode` and `partnerCode` and
  deliberately **not** `partner2Code`. Do not "complete" that tuple.
  The lesson is more general: a code is not an aggregate just because it
  appears in the areas table.
- **`490` is Taiwan** and `exclude_pseudo` keeps it. UNSD cannot name it, so
  publishes it as "Other Asia, nes". A single reporting economy, not a sum of
  countries. Do not fold it into the aggregates because its label looks like
  one.
- **The preflight counts are pre-filter, deliberately.** The ceiling guards the
  API *response*, which is unfiltered, so counting unfiltered is the
  conservative choice. Consequence: `expected_rows_total` is an upper bound of
  the sidecar's `rows`, and should be compared to `rows_received` instead.
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

All counts below are **pre-filter**: `exclude_pseudo` runs after the download,
so it does not move any of these. Re-measured on 2026-10-25 with hash
`ac3dd013`, identical to the figures taken under `8dbfc6dd`.

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
```

First real download, 2026-10-25, free key, hash 947dc53d:

```
2000-2003, code 4403, M+X, all reporters   4 free counting calls, 1 keyed call
  per year: 4,219 | 4,182 | 4,211 | 4,322
  expected 16,934  ->  RECEIVED 16,934     exact, the plan predicts the download
  exclude_pseudo: 16,934 -> 15,630 (1,304 dropped, 7.7%)
    partner 0 World 1,129 | 899 Areas nes 124 | 838 Free Zones 18
    568 Other Europe nes 11 | 577 Other Africa nes 6 | 837 Bunkers 6
  delivered: 175 reporters, 223 partners, 47 columns, Taiwan (490) present
  mirror: 10,996 directed edges, 4,634 (42%) declared on both sides,
          3,984 with no exporter declaration, 2,378 with no importer one
```

Orders of magnitude, all reporters, M+X:

```
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
  empty packs and absent periods are caught today, plus the ISO3 double-count
  warning added on 2026-10-25. The availability `totalRecords` is not a usable
  baseline (whole reporter-period, not our codes); the preview counts are. See
  README for the full note. **Compare against the sidecar's `rows_received`,
  not `rows`**: the latter is post-`exclude_pseudo`, so it is legitimately
  lower than the plan.
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
