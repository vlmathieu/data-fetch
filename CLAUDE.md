# CLAUDE.md

Guidance for Claude Code in `data-fetch`. **Lire [README.md](README.md) d'abord** :
il dit ce que fait le repo et pourquoi. Ce fichier dit comment y travailler.

## État réel (2026-10-24, commit cc3ead8)

| Règle | État |
|---|---|
| `comtrade_preflight` | **vérifiée de bout en bout** contre l'API réelle |
| `comtrade_download` | **jamais exécutée** — exige la clé premium |
| `comtrade_mirror` | **jamais exécutée** — dépend de la sortie de `download` |

Syntaxe validée et DAG valide pour les trois, mais ça ne vaut pas exécution.
Le premier vrai tirage est à faire par l'utilisateur. **Ne jamais affirmer que
`download` ou `mirror` fonctionne** — dire qu'ils n'ont pas tourné.

## Commands

```zsh
python workflow/scripts/comtrade_common.py        # seul contrôle exécutable
snakemake preflight --sdm conda --cores 1         # gratuit, sans clé
snakemake --sdm conda --cores 1                   # tirage complet (quota premium)
snakemake -n                                      # dry-run, pas besoin de conda
snakemake --sdm conda --cores 1 --config run_id=20261024T120000   # rejoue un tirage
```

**Pas de suite de tests.** Vérification = lancer la règle, lire
`workflow/logs/<règle>_<stem>.log`, inspecter `data/comtrade/`.

`comtrade_common.py` a un `_demo()` sous `__main__` : hash stable, empaquetage,
refus de `TOTAL` et de `hs_revision: auto`. Toute logique non triviale ajoutée
là y laisse un `assert`.

## Architecture

```
Snakefile ──(import au moment du DAG)──> comtrade_common.resolve_params, params_hash, stem
    │                                     └─ fixe STEM, donc tous les chemins de sortie
    └─ include rules/comtrade.smk
         comtrade_preflight  → {STEM}.plan.json          endpoints PUBLICS, hors quota
         comtrade_download   → {STEM}.{parquet,csv} + {STEM}.meta.json   quota premium
         comtrade_mirror     → {STEM}.mirror.{parquet,csv}
```

`comtrade_common.py` est partagé entre le Snakefile et les scripts. Il contient
les plafonds (`FAIL_ROWS`/`WARN_ROWS`), les constantes `FIXED`, la résolution et
le hachage des paramètres, l'enveloppe `call()`, `selection()`, `pack_periods()`
et `bisect_codes()`.

## Gotchas

Tous trouvés en exécutant, pas en relisant.

- **Jamais de `from __future__ import annotations` dans un fichier `script:`.**
  Snakemake insère un préambule avant le contenu du fichier, donc l'import
  n'est plus en première ligne → `SyntaxError`. L'env est en Python 3.12 :
  `str | None` et `list[str]` marchent nativement. `comtrade_common.py` est un
  module importé, pas un `script:`, donc lui peut le garder.
- **`--config` avale tous les arguments suivants.** La cible va **avant** :
  `snakemake preflight --cores 1 --config run_id=…`, jamais l'inverse.
- **`_getFinalDataAvailability(typeCode, freqCode, clCode, period, reporterCode)`**
  — `typeCode` en premier. L'oublier ne lève qu'à l'exécution.
- **Les chemins de sortie dépendent de `params_hash`, calculé au moment du DAG.**
  Toucher `resolve_params()` ou `_NOT_IN_HASH` renomme **tous** les fichiers
  futurs et détache les tirages passés de leur requête. Ce n'est pas un
  refactor gratuit.
- **`comtrade_common.py` doit rester léger à l'import** : le Snakefile l'importe
  avant toute règle. Pas de polars, pas de comtradeapicall dedans.
- **`comtradeapicall` renvoie du pandas.** Conversion `pl.from_pandas()` à la
  frontière, dans les scripts ; tout le reste est en polars.
- **`period.stop` est INCLUSIF.** Correction délibérée du `stop - 2` de
  `trade_network_analysis`. Ne pas « harmoniser » à l'ancien comportement.
- **`parquet` est forcé dans `FORMATS`** même si la config ne le demande pas :
  `comtrade_mirror` lit le parquet, jamais le CSV.
- **Le lien `latest_<hash>` est écrit par `comtrade_download.py`**, pas par une
  règle. Indexé par hash, parce qu'un `latest` global devient faux dès qu'il y
  a deux requêtes dans le dossier.

## Pièges de `comtradeapicall`

Vérifiés sur la 1.2.1 installée en local et la source `main` (1.3.2, épinglée).

- **`getFinalData` tronque en silence à 250 000 enregistrements.** Aucune
  erreur au plafond. D'où l'échec dur dans `fetch_pack()`.
- **Les erreurs HTTP sont avalées** : `getPreviewData` imprime le corps et
  retourne `None` sans lever. Le `except:` de `_getFinalData` ne part donc
  jamais sur un 429, et `pandas.concat` **ignore un `None` sans erreur**
  (vérifié sur pandas 2.2.2 et 3.0.1) : une période en échec disparaîtrait du
  résultat sans trace. **Tout appel passe par `call()`**, qui traite `None`
  comme un échec. Ne jamais appeler le paquet directement.
- **Le preview public n'accepte qu'une période par appel** :
  `{"error":"Maximum number of periods for preview is 1"}`. D'où un appel de
  comptage par période. Gratuit, donc sans importance.
- **Le comptage public renvoie le compte réel, non plafonné.** Le
  « limited to 500 records » du README ne porte que sur les lignes de données.
- **Async non implémenté, délibérément.** Plafond à 2,5 M, mais
  `downloadAsyncFinalDataRequest` boucle sans timeout ni nombre max de
  tentatives, laisse le serveur nommer le fichier — donc aucun `output:`
  déclarable — et plante en `TypeError` si la soumission échoue. L'ouvrir
  exige de réécrire submit / check / download à la main.

## Sécurité

- La clé est lue **dans `comtrade_download.py`**, via `os.environ`. **Jamais en
  `params:`** : Snakemake enregistre tous les params en clair dans
  `.snakemake/metadata/`. C'est exactement la fuite trouvée dans
  `trade_network_analysis`.
- **Pas de directive `envvars:`** dans le Snakefile, volontairement : elle
  ferait échouer tout le workflow sans clé, y compris le pré-vol.
- **`comtrade_preflight` est la seule règle lançable sans risque** par un agent :
  endpoints publics, aucune clé, aucun quota. `comtrade_download` consomme du
  quota premium payant — ne pas le lancer sans demande explicite.
- La clé n'apparaît jamais dans le sidecar, les logs ou un message d'erreur.
  Si du code logue une URL de requête, la masquer : `subscription-key` voyage
  en query-string.

## Chiffres de référence mesurés

Pour repérer un tirage anormal.

```
Config par défaut (1996-2023, codes 4403/4407/4412, M+X, tous reporters) :
  539 660 lignes attendues,  28 appels gratuits,  3 appels premium
  paquets : 1996-2006 → 187 296 | 2007-2015 → 184 257 | 2016-2023 → 168 107
  millésime : 4 756 enregistrements (reporter × période)

Ordres de grandeur, tous reporters, M+X :
  France 2015, TOUS produits  → 674 847   (d'où le refus de cmd: TOTAL)
  23 codes bois, 1996          →  33 092
  56 codes bois, 2017          →  95 289
  67 codes bois, 2023          → 101 252
```

Comtrade réécrit l'historique : France 2020 publié le 2021-05-26, révisé le
2026-05-23. Deux tirages identiques peuvent donc légitimement différer — le
`datasetChecksum` du sidecar tranche.

## Conventions

- Config commentée dans [config/config.yaml](config/config.yaml). Un paramètre
  qui ne varie jamais va dans `FIXED`, pas dans la config.
- Commentaires en français dans le code et la doc ; messages de commit en
  anglais (convention de l'utilisateur).
- Flux miroirs : **jamais réconciliés**, sommes gardées pour que les nuls
  restent nuls. L'écart entre déclarations est un signal, pas du bruit.
- Refus explicites plutôt qu'échecs obscurs plus tard : `cmd: TOTAL` et
  `hs_revision: auto` lèvent dans `resolve_params()`.

## Chantiers

- **`mapping`** — `hs_revision: auto` et HS → FAO. Exige une table de
  correspondance par révision. En `strict`, un code absent d'une révision
  renvoie zéro ligne et produit un avertissement, rien de plus.
- **async** — à ouvrir seulement si une sélection légitime dépasse 250 K.
- **autres sources** — Banque mondiale, FAOSTAT. Même contrat : pré-vol
  gratuit, fichier daté, sidecar.

## Rapport avec `trade_network_analysis`

Repo voisin (`../trade_network_analysis`), source de ce code. **Y est resté
volontairement**, ne pas recopier :

- `process_param()` — découpage par révision HS ; sert au chantier `mapping`,
  pas à la v1 `strict`.
- `correspondence_FAO_HS.{py,json}` — table FAO↔HS ; même raison.

Repris : la jointure miroir de `network_objects.py`, l'env conda (1.2.1 →
1.3.2), les motifs d'ignore. `get_uncomtrade_data.py` n'a servi que de
référence pour la forme de l'appel.

**Ne pas modifier `trade_network_analysis`** : le manuscrit est soumis, son pin
`comtradeapicall ==1.2.1` et ses chemins restent figés.
