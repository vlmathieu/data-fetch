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

    Reads comtrade_apikey from the environment, inside the script: never in
    `params:`, which Snakemake records in cleartext in .snakemake/metadata/.
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
