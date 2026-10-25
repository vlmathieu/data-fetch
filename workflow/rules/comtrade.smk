from comtrade_common import plan

# One call derives everything from the config block: resolved request,
# fingerprint, stem, formats, and the files to produce. Its refusals (cmd
# TOTAL, an unknown ISO3, a ceiling above the tier) fire HERE, at DAG time,
# before a single row is downloaded.
CT = plan(config["comtrade"], RUN_ID)
TARGETS += CT["targets"]

# Short names, for the rules below to stay readable. Safe even with several
# sources: a rule's f-strings are evaluated when the rule is PARSED, so a
# later include reassigning these names cannot affect rules already declared.
PARAMS, STEM, OUT = CT["params"], CT["stem"], CT["out"]
FORMATS, CSV_SEP = CT["formats"], CT["csv_sep"]


rule preflight:
    """Short target: `snakemake preflight` sizes the run without downloading.

    Comtrade-specific, hence here rather than in the Snakefile.
    """
    input:
        f'{OUT}/{STEM}.plan.json'


rule comtrade_preflight:
    """Counts rows and records the vintage. Public endpoints: off quota.

    No API key. This is the only rule you can run without risk of spending
    premium quota.
    """
    output:
        f'{OUT}/{STEM}.plan.json'
    params:
        params = PARAMS,
        run_id = RUN_ID
    log:
        f'workflow/logs/comtrade_preflight_{STEM}.log'
    threads: 1
    conda:
        '../envs/comtrade.yaml'
    script:
        '../scripts/comtrade_preflight.py'


rule comtrade_download:
    """Downloads according to the plan. The only rule that spends premium quota.

    Reads comtrade_apikey from the environment, inside the script.
    """
    input:
        f'{OUT}/{STEM}.plan.json'
    output:
        [f'{OUT}/{STEM}.{fmt}' for fmt in FORMATS] + [f'{OUT}/{STEM}.meta.json']
    params:
        csv_sep = CSV_SEP
    log:
        f'workflow/logs/comtrade_download_{STEM}.log'
    threads: 1
    conda:
        '../envs/comtrade.yaml'
    script:
        '../scripts/comtrade_download.py'


rule comtrade_mirror:
    """Both declarations of one edge, side by side. Never reconciled."""
    input:
        f'{OUT}/{STEM}.parquet'
    output:
        [f'{OUT}/{STEM}.mirror.{fmt}' for fmt in FORMATS]
    params:
        csv_sep = CSV_SEP
    log:
        f'workflow/logs/comtrade_mirror_{STEM}.log'
    threads: 4
    conda:
        '../envs/comtrade.yaml'
    script:
        '../scripts/mirror_flows.py'


rule comtrade_eu:
    """The EU as one node, summed from its member states. A DERIVED file.

    Never Comtrade's code 97: that code exists only as a reporter and reports
    nothing before 2000. The canonical parquet is left untouched, so the
    aggregation can be redone without downloading anything again.
    """
    input:
        f'{OUT}/{STEM}.parquet'
    output:
        [f'{OUT}/{STEM}.eu.{fmt}' for fmt in FORMATS]
    params:
        params = PARAMS,
        csv_sep = CSV_SEP
    log:
        f'workflow/logs/comtrade_eu_{STEM}.log'
    threads: 4
    conda:
        '../envs/comtrade.yaml'
    script:
        '../scripts/aggregate_eu.py'
