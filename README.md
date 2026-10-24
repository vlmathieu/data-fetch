# data-fetch

Téléchargements de données datés, tracés et reproductibles. Chaque exécution
produit un fichier horodaté à nom unique, accompagné d'un *sidecar* qui dit
exactement ce qui a été demandé et quelle version des données a été servie.

Source disponible : **UN Comtrade**. Le repo est fait pour en accueillir
d'autres — une source = un fichier de règles + un bloc de config.

## Démarrage

```zsh
# 1. Chiffrer le tirage sans rien télécharger ni consommer de quota
snakemake preflight --sdm conda --cores 1

# 2. Lire le plan, puis tirer
snakemake --sdm conda --cores 1

# 3. Rejouer un tirage précis (reproductible)
snakemake --sdm conda --cores 1 --config run_id=20261024T120000
```

Le pré-vol passe uniquement par les endpoints **publics** de Comtrade : aucune
clé, aucun quota. C'est la seule étape qu'on peut lancer sans risque.

## Clé API

Le téléchargement exige une clé d'abonnement UN Comtrade **premium** : il passe
par `getFinalData`, pas par l'endpoint public. La clé s'obtient sur
[comtradedeveloper.un.org](https://comtradedeveloper.un.org) — créer un compte,
puis souscrire au produit *comtrade-v1* et relever la *primary key*.

Le workflow la lit dans la variable d'environnement **`comtrade_apikey`**, et
uniquement là. Elle n'apparaît dans aucun fichier de configuration.

### Poser la variable

Le plus simple, pour une session de terminal :

```zsh
export comtrade_apikey="votre-cle"
snakemake --sdm conda --cores 1
```

Pour ne pas la retaper à chaque session, zsh la lit depuis `~/.zshrc` — mais
elle est alors en clair dans un fichier, et présente dans l'environnement de
tous vos processus. Préférable : la ranger dans le Keychain macOS et la sortir
à la demande.

```zsh
# une seule fois
security add-generic-password -a "$USER" -s comtrade_apikey -w

# à chaque session, ou dans une fonction de ~/.zshrc
export comtrade_apikey="$(security find-generic-password -a "$USER" -s comtrade_apikey -w)"
```

Sous Linux, `pass` avec gpg-agent joue le même rôle ; `op read "op://…"` avec
1Password CLI fonctionne partout.

### Vérifier sans consommer de quota

Le pré-vol n'utilise **aucune clé** : il interroge les endpoints publics de
Comtrade. Il tourne donc avant même d'avoir souscrit, et chiffre le tirage :

```zsh
snakemake preflight --sdm conda --cores 1
cat data/comtrade/*.plan.json
```

Le plan donne le nombre de lignes attendues, le découpage retenu et le nombre
d'appels premium que le tirage coûtera.

### Ce que la clé ne doit jamais toucher

- Elle est lue **dans** `comtrade_download.py`, jamais déclarée en `params:`
  d'une règle Snakemake.
- Elle n'est écrite ni dans le sidecar, ni dans les logs, ni dans un message
  d'erreur. Si vous ajoutez du code qui journalise une URL de requête,
  masquez-la : `subscription-key` voyage en query-string.
- `data/` et `.snakemake/` sont dans `.gitignore` **et** `.dockerignore` depuis
  le premier commit.

Et une limite à connaître : aucun mécanisme de fichier ou de variable
d'environnement ne cache un secret à un processus tournant sous votre compte.
Ce qui protège réellement, c'est un coffre à déverrouillage interactif
(Keychain, 1Password, `pass`), plus la règle simple que seul
`comtrade_preflight` est lançable sans risque — `comtrade_download`, lui,
consomme du quota payant.

## Paramètres

Tout est dans [config/config.yaml](config/config.yaml), commenté.

Figés hors config, dans `FIXED` de
[comtrade_common.py](workflow/scripts/comtrade_common.py) : `typeCode=C`
(biens), `breakdownMode=classic`, `customsCode`/`motCode`/`maxRecords`/
`proxy_url` à `None`. Un paramètre qui ne varie jamais est un paramètre à
supprimer.

Deux refus explicites :

- **`cmd: TOTAL`** — une seule paire reporter-année dépasse déjà le plafond
  (France 2015, tous produits : 674 847 lignes).
- **`hs_revision: auto`** — exige une table de correspondance par révision
  (chantier `mapping`). La v1 est en `strict` : les codes sont passés tels
  quels, avec un avertissement sur chaque période qui renvoie zéro ligne.

## Découpage et plafond

`getFinalData` plafonne à **250 000 enregistrements** et **tronque sans
erreur** au plafond. Le découpage n'est donc pas une constante devinée, il est
mesuré :

1. un compte **gratuit** par période, via l'endpoint public — le compte renvoyé
   est le compte réel, non plafonné ;
2. empaquetage glouton de périodes consécutives sous **200 000** lignes (marge
   pour les révisions amont entre le pré-vol et le tirage) ;
3. si une période seule dépasse, bissection sur les codes, toujours au compte
   gratuit ;
4. échec dur à **250 000** lignes reçues, alerte dès 200 000.

Les comptes sont additifs — une requête sur P1..Pn renvoie l'union des lignes —
donc la somme des comptes par période *est* le compte du paquet. C'est ce qui
permet de planifier à l'optimum sans tirer une ligne.

Mesuré sur 1996–2023, codes 4403/4407/4412 : **539 660 lignes, 3 appels
premium** (et 28 appels de comptage gratuits).

## Sorties

```
data/comtrade/
  comtrade_HS_A_8dbfc6dd_20261024T120000.parquet         # artefact canonique
  comtrade_HS_A_8dbfc6dd_20261024T120000.csv             # livrable collègues
  comtrade_HS_A_8dbfc6dd_20261024T120000.meta.json       # sidecar
  comtrade_HS_A_8dbfc6dd_20261024T120000.mirror.parquet  # si mirror: true
  comtrade_HS_A_8dbfc6dd_20261024T120000.mirror.csv
  latest_8dbfc6dd.parquet -> ...                         # lien par hash
```

`8dbfc6dd` est une empreinte SHA-256 tronquée du jeu de paramètres **résolu**,
triée et normalisée, hors clés de livraison (`format`, `csv_sep`, `mirror`,
`run_id`). Elle répond à la seule question qu'un horodatage ne tranche pas :
deux fichiers sont-ils le même tirage à deux dates — donc comparables — ou deux
requêtes différentes ?

```zsh
ls data/comtrade/*.parquet | cut -d_ -f4 | sort -u   # requêtes distinctes
ls data/comtrade/*8dbfc6dd*                          # tous les tirages de l'une
```

Le lien `latest_` est indexé par hash : un `latest` global devient faux dès
qu'il y a deux requêtes dans le dossier.

### Sidecar

Un par fichier de données. Les paramètres résolus d'un côté, l'état des données
en amont de l'autre — `classificationCode`, `totalRecords`, `datasetChecksum`
et `lastReleased` par (reporter, période).

Ce second volet est le moins évident et le plus utile : **Comtrade réécrit
l'historique**. France 2020 a été publié le 2021-05-26 puis révisé le
2026-05-23. Sans ce relevé, deux tirages du même paramétrage qui divergent sont
inexplicables, et on soupçonne son propre code. `datasetChecksum` est l'ancre
la plus forte : inchangé = données identiques.

La clé API n'y figure jamais.

## Lire le CSV (Stata, R)

Le CSV est le seul format que Stata et R lisent tous les deux sans installer de
paquet. Il a deux coûts.

**Taille** : environ 11× le parquet.

**Types perdus** : `"01"` relu devient `1`. Le pipeline ne lit jamais le CSV —
le parquet est canonique — mais vos collègues, si :

```r
read.csv("fichier.csv", colClasses = c(cmdCode = "character"))
```
```stata
import delimited "fichier.csv", stringcols(_all)
```

Les noms de pays contenant une virgule (`China, Hong Kong SAR`) sont quotés
automatiquement par polars et survivent au séparateur virgule. Pas besoin de
séparateur exotique.

## Flux miroirs

Un flux bilatéral est déclaré deux fois : par l'exportateur (`X`) et par
l'importateur (`M`). Le fichier miroir les met côte à côte, suffixés `_exp` et
`_imp`, une ligne par arête dirigée `(period, cmdCode, exporterISO,
importerISO)`.

Deux règles non négociables :

- **Jamais de réconciliation.** Pas de moyenne FOB/CIF, pas de substitution
  d'un côté par l'autre. L'écart entre les deux déclarations est un signal de
  qualité de la donnée.
- **Les nuls restent nuls, pas zéro.** Les sommes sont gardées : un groupe
  entièrement nul reste nul. Sinon un non-déclarant devient un déclarant à
  zéro et le diagnostic disparaît.

## Pièges de `comtradeapicall`

Vérifiés sur la 1.2.1 installée et la source `main` (1.3.2, épinglée ici).

**Les erreurs HTTP sont avalées.** `getPreviewData` imprime le corps de la
réponse et retourne `None` sans lever. Le `except:` de `_getFinalData` ne part
donc jamais sur un 429 ou un 503. Et `pandas.concat` **ignore un `None` sans
erreur** (vérifié sur pandas 2.2.2 et 3.0.1) : une période en échec
disparaîtrait du résultat sans laisser de trace. D'où `call()` dans
`comtrade_common.py`, qui traite un retour `None` comme un échec et lève après
épuisement des tentatives. Tout appel passe par là.

**Le preview n'accepte qu'une période par appel** —
`{"error":"Maximum number of periods for preview is 1"}`. Sans conséquence,
c'est gratuit.

**L'async n'est pas implémenté ici, et c'est délibéré.** Il lèverait le plafond
à 2,5 M, mais `downloadAsyncFinalDataRequest` boucle sans timeout ni nombre max
de tentatives, laisse le serveur choisir le nom du fichier — donc aucun
`output:` déclarable — et plante en `TypeError` si la soumission échoue.

## Contrôle

```zsh
python workflow/scripts/comtrade_common.py   # hash stable, empaquetage, refus
```

Vérifie que le hash ignore l'ordre des listes et les types mais pas un vrai
changement de requête, que `period.stop` est inclusif, qu'aucune période n'est
perdue à l'empaquetage, et que `TOTAL` comme `hs_revision: auto` sont refusés.

## Chantiers

- **`mapping`** — `hs_revision: auto` et la conversion HS → FAO. Demande une
  table de correspondance par révision. La table FAO existe dans
  `trade_network_analysis` (`resources/inhouse/correspondence_FAO_HS.json`) ;
  le cas général exige les tables de corrélation UNSD H0↔H6.
- **async** — à ouvrir seulement si une sélection légitime dépasse 250 K.
  Implique de réécrire submit / check / download à la main.
- **autres sources** — Banque mondiale, FAOSTAT. Même contrat : pré-vol
  gratuit, fichier daté, sidecar.
