rule comtrade_preflight:
    """Compte les lignes et relève le millésime. Endpoints publics : hors quota.

    Aucune clé API. C'est la seule règle qu'on peut lancer sans risque de
    consommer du quota premium.
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
    """Tirage selon le plan. Seule règle qui consomme du quota premium.

    Lit comtrade_apikey dans l'environnement, à l'intérieur du script : jamais
    en `params:`, que Snakemake enregistre en clair dans .snakemake/metadata/.
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
    """Les deux déclarations d'une même arête, côte à côte. Jamais réconciliées."""
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
