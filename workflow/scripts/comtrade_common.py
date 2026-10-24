"""Helpers partagés : résolution des paramètres, hash stable, enveloppe d'appel.

Importé par le Snakefile (pour calculer les chemins de sortie au moment du DAG)
et par les scripts de règle.
"""

from __future__ import annotations

import hashlib
import json
import time

# --- plafonds de l'API ------------------------------------------------------
# getFinalData est plafonné à 250 000 enregistrements (README du paquet). Une
# réponse au plafond est tronquée SANS erreur : on échoue au lieu de livrer.
FAIL_ROWS = 250_000
# Marge : les comptes du pré-vol peuvent bouger avant le tirage (révisions amont).
WARN_ROWS = 200_000

# --- paramètres figés -------------------------------------------------------
# Hors config, volontairement : ils ne varient pas dans ce repo.
FIXED = {
    "type": "C",                  # biens uniquement ; 'S' (services) hors périmètre
    "breakdown_mode": "classic",  # 'plus' n'est utile qu'avec customs/mot
    "customs": None,              # n'existe qu'en breakdown 'plus'
    "mot": None,                  # idem
    "max_records": None,          # aucun troncage voulu
    "proxy_url": None,            # pas de proxy institutionnel
}

# Clés qui décrivent la livraison, pas la requête : exclues du hash.
_NOT_IN_HASH = frozenset({"format", "csv_sep", "mirror", "run_id", "out_dir"})


# --- résolution des paramètres ---------------------------------------------

def _norm_list(value) -> list[str] | None:
    """Liste canonique de chaînes, triée et dédupliquée. None reste None."""
    if value is None:
        return None
    if isinstance(value, (str, int)):
        value = [value]
    out = sorted({str(v).strip() for v in value})
    return out or None


def _norm_selector(value) -> list[str] | None:
    """'all' et None valent None (= tous), sinon une liste canonique."""
    if value is None or (isinstance(value, str) and value.strip().lower() == "all"):
        return None
    return _norm_list(value)


def _periods(cfg: dict) -> list[str]:
    period = cfg.get("period") or {}
    if period.get("list"):
        return sorted(str(p).strip() for p in period["list"])
    if str(cfg.get("freq", "A")).upper() == "M":
        raise ValueError("freq: M exige comtrade.period.list, en YYYYMM")
    start, stop = int(period["start"]), int(period["stop"])
    if stop < start:
        raise ValueError(f"period.stop ({stop}) < period.start ({start})")
    return [str(y) for y in range(start, stop + 1)]  # stop INCLUS


def resolve_params(cfg: dict) -> dict:
    """Applique les défauts et normalise. C'est ce dict qui est haché et stocké.

    Hacher les paramètres *résolus* et non ceux écrits : sans ça, deux configs
    équivalentes (ordre des listes, entier vs chaîne, défaut omis) donneraient
    deux hash différents pour la même requête.
    """
    cmd = _norm_list(cfg.get("cmd"))
    if not cmd:
        raise ValueError("comtrade.cmd est obligatoire")
    if any(c.upper() == "TOTAL" for c in cmd):
        raise ValueError(
            "cmd: TOTAL est refusé — une seule paire reporter-année dépasse "
            "déjà le plafond de 250 K (France 2015 : 674 847 lignes)."
        )

    params = {
        "data_type":      str(cfg.get("data_type", "final")).lower(),
        "freq":           str(cfg.get("freq", "A")).upper(),
        "classification": str(cfg.get("classification", "HS")).upper(),
        "period":         _periods(cfg),
        "cmd":            cmd,
        "flow":           _norm_list(cfg.get("flow", ["M", "X"])),
        "reporter":       _norm_selector(cfg.get("reporter", "all")),
        "partner":        _norm_selector(cfg.get("partner", "all")),
        "partner2":       _norm_selector(cfg.get("partner2")),
        "aggregate_by":   cfg.get("aggregate_by") or None,
        "include_desc":   bool(cfg.get("include_desc", True)),
        "hs_revision":    str(cfg.get("hs_revision", "strict")).lower(),
    }

    if params["data_type"] not in {"final", "tariffline"}:
        raise ValueError("data_type doit valoir 'final' ou 'tariffline'")
    if params["hs_revision"] == "auto":
        raise NotImplementedError(
            "hs_revision: auto exige une table de correspondance par révision "
            "(chantier `mapping`). Utiliser 'strict' en v1."
        )
    if params["hs_revision"] != "strict":
        raise ValueError("hs_revision doit valoir 'strict' ou 'auto'")
    return params


def params_hash(params: dict, length: int = 8) -> str:
    """Empreinte courte et stable du jeu de paramètres résolu.

    hashlib, jamais hash() : le hash() de Python est randomisé par processus sur
    les chaînes (PYTHONHASHSEED), donc il changerait à chaque exécution.
    """
    payload = {k: v for k, v in params.items() if k not in _NOT_IN_HASH}
    canon = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()[:length]


def stem(params: dict, run_id: str) -> str:
    """Radical commun au fichier de données, au CSV et au sidecar."""
    return (
        f"comtrade_{params['classification']}_{params['freq']}"
        f"_{params_hash(params)}_{run_id}"
    )


# --- enveloppe d'appel ------------------------------------------------------

def call(fn, *args, attempts: int = 5, base_delay: float = 5.0,
         label: str = "", **kwargs):
    """Appelle une fonction comtradeapicall en traitant un retour None comme un échec.

    Indispensable : le paquet avale les erreurs HTTP. getPreviewData imprime le
    corps de la réponse et retourne None sans lever, et pandas.concat ignore
    ensuite ce None sans erreur (vérifié sur pandas 2.2.2 et 3.0.1). Une période
    en 429 disparaîtrait donc du résultat sans laisser de trace.
    """
    detail = "aucune tentative"
    for attempt in range(attempts):
        try:
            out = fn(*args, **kwargs)
            if out is not None:
                return out
            detail = "réponse None (erreur HTTP avalée par comtradeapicall)"
        except Exception as exc:                      # noqa: BLE001
            detail = f"{type(exc).__name__}: {exc}"
        if attempt < attempts - 1:
            time.sleep(base_delay * 2 ** attempt)     # 5s, 10s, 20s, 40s
    raise RuntimeError(f"échec après {attempts} tentatives [{label}] : {detail}")


# --- critères de sélection --------------------------------------------------

def join(values) -> str | None:
    """Liste -> chaîne séparée par des virgules. None (= tous) reste None."""
    return None if values is None else ",".join(values)


def selection(params: dict, period: str, cmd: list[str] | None = None) -> dict:
    """Arguments de sélection, communs au comptage et au tirage.

    Les deux doivent utiliser exactement les mêmes, sinon le compte ne décrit
    pas ce qui sera réellement téléchargé.
    """
    args = dict(
        typeCode=FIXED["type"],
        freqCode=params["freq"],
        clCode=params["classification"],
        period=period,
        reporterCode=join(params["reporter"]),
        cmdCode=join(cmd if cmd is not None else params["cmd"]),
        flowCode=join(params["flow"]),
        partnerCode=join(params["partner"]),
        partner2Code=join(params["partner2"]),
        customsCode=FIXED["customs"],
        motCode=FIXED["mot"],
    )
    if params["data_type"] == "final":
        args["breakdownMode"] = FIXED["breakdown_mode"]
        args["aggregateBy"] = params["aggregate_by"]
    return args


# --- planification du découpage --------------------------------------------

def pack_periods(counts: dict[str, int], limit: int = WARN_ROWS) -> list[list[str]]:
    """Empaquette des périodes consécutives en gardant la somme sous `limit`.

    Les comptes sont additifs : une requête sur P1..Pn renvoie l'union des
    lignes, donc la somme des comptes par période EST le compte du paquet.
    C'est ce qui permet de planifier à l'optimum sans tirer une seule ligne.

    Une période seule au-dessus de `limit` part dans son propre paquet ; c'est
    au tirage de la scinder sur les codes (cf. bisect_codes).
    """
    packs: list[list[str]] = []
    current: list[str] = []
    total = 0
    for period in sorted(counts):
        n = counts[period]
        if current and total + n > limit:
            packs.append(current)
            current, total = [], 0
        current.append(period)
        total += n
    if current:
        packs.append(current)
    return packs


def bisect_codes(codes: list[str], count_fn, limit: int = WARN_ROWS
                 ) -> list[tuple[list[str], int]]:
    """Scinde une liste de codes jusqu'à ce que chaque groupe tienne sous `limit`.

    `count_fn(codes) -> int` doit passer par le comptage gratuit. Chemin
    d'exception : avec TOTAL refusé, aucune sélection courante n'y arrive.
    """
    n = count_fn(codes)
    if n < limit or len(codes) == 1:
        return [(codes, n)]
    middle = len(codes) // 2
    return (bisect_codes(codes[:middle], count_fn, limit)
            + bisect_codes(codes[middle:], count_fn, limit))


# --- contrôle ---------------------------------------------------------------

def _demo() -> None:
    """Contrôle exécutable du hash et de l'empaquetage : python comtrade_common.py"""
    base = {"period": {"start": 1996, "stop": 1998}, "cmd": ["4403", "4407"]}

    # Le hash ignore l'ordre des listes, le type, et les clés de livraison.
    a = resolve_params(base)
    b = resolve_params({"period": {"start": 1996, "stop": 1998},
                        "cmd": [4407, "4403"], "flow": ["X", "M"]})
    assert params_hash(a) == params_hash(b), "hash sensible à l'ordre ou au type"

    c = resolve_params({**base, "classification": "H5"})
    assert params_hash(a) != params_hash(c), "hash insensible à un vrai changement"

    # stop est inclusif.
    assert a["period"] == ["1996", "1997", "1998"], a["period"]

    # Le hash est stable entre processus (valeur en dur : doit rester identique).
    assert params_hash(a) == hashlib.sha256(
        json.dumps({k: v for k, v in a.items() if k not in _NOT_IN_HASH},
                   sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()[:8]

    # Empaquetage : somme sous la limite, ordre chronologique préservé.
    counts = {"1996": 90, "1997": 90, "1998": 90, "1999": 10}
    assert pack_periods(counts, limit=200) == [["1996", "1997"], ["1998", "1999"]]
    # Une période seule au-dessus de la limite reste seule.
    assert pack_periods({"1996": 500, "1997": 10}, limit=200) == [["1996"], ["1997"]]
    # Aucune période perdue, quel que soit le découpage.
    assert sorted(p for pk in pack_periods(counts, 200) for p in pk) == sorted(counts)

    # Bissection des codes.
    sizes = {"a": 150, "b": 150, "c": 10, "d": 10}
    got = bisect_codes(["a", "b", "c", "d"], lambda cs: sum(sizes[x] for x in cs), 200)
    assert all(n < 200 for _, n in got), got
    assert sorted(x for cs, _ in got for x in cs) == ["a", "b", "c", "d"]

    # TOTAL refusé.
    try:
        resolve_params({**base, "cmd": ["TOTAL"]})
    except ValueError:
        pass
    else:
        raise AssertionError("TOTAL devrait être refusé")

    # hs_revision: auto non implémenté.
    try:
        resolve_params({**base, "hs_revision": "auto"})
    except NotImplementedError:
        pass
    else:
        raise AssertionError("hs_revision: auto devrait lever NotImplementedError")

    print("comtrade_common : contrôles OK")


if __name__ == "__main__":
    _demo()
