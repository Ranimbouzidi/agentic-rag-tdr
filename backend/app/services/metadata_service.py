from __future__ import annotations

import re
from datetime import datetime
from typing import Optional, Dict, Any


# -----------------------------
# Helpers
# -----------------------------
MONTHS_FR = {
    "janvier": 1, "janv": 1,
    "février": 2, "fevrier": 2, "fév": 2, "fev": 2,
    "mars": 3,
    "avril": 4,
    "mai": 5,
    "juin": 6,
    "juillet": 7,
    "août": 8, "aout": 8,
    "septembre": 9, "sept": 9,
    "octobre": 10, "oct": 10,
    "novembre": 11, "nov": 11,
    "décembre": 12, "decembre": 12, "déc": 12, "dec": 12,
}

def _to_iso_date(y: int, m: int, d: int) -> Optional[str]:
    try:
        return datetime(y, m, d).date().isoformat()
    except Exception:
        return None

def _norm(s: str) -> str:
    s = (s or "").lower()
    s = s.replace("’", "'")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _first_match(regex: str, text: str, flags: int = re.IGNORECASE) -> Optional[re.Match]:
    return re.search(regex, text, flags=flags)

def _find_all(regex: str, text: str, flags: int = re.IGNORECASE) -> list[re.Match]:
    return list(re.finditer(regex, text, flags=flags))


# -----------------------------
# Langue (heuristique simple)
# -----------------------------
FR_MARKERS = ["le", "la", "les", "des", "pour", "avec", "dans", "afin", "ministère", "république", "termes de référence"]
EN_MARKERS = ["the", "and", "for", "with", "within", "background", "scope of work", "terms of reference", "deadline"]

def detect_language(text: str) -> Optional[str]:
    t = _norm(text)
    if not t:
        return None
    fr = sum(1 for w in FR_MARKERS if w in t)
    en = sum(1 for w in EN_MARKERS if w in t)
    if fr == 0 and en == 0:
        return None
    return "fr" if fr >= en else "en"


# -----------------------------
# Bailleur / financeur
# -----------------------------
BAILLEURS = [
    ("banque mondiale", ["banque mondiale", "world bank"]),
    ("bird", ["bird", "ibrd", "prêt no.", "pret no.", "loan no."]),
    ("afd", ["afd", "agence française de développement", "agence francaise de developpement"]),
    ("ue", ["union européenne", "union europeenne", "ue", "europa", "european union"]),
    ("bad", ["banque africaine de développement", "banque africaine de developpement", "bad", "afdb"]),
    ("giz", ["giz", "deutsche gesellschaft für internationale zusammenarbeit"]),
    ("un", ["unicef", "undp", "pnu d", "pnud", "unfpa", "onu", "nations unies"]),
]

def detect_bailleur(text: str) -> Optional[str]:
    t = _norm(text)
    for canon, kws in BAILLEURS:
        for kw in kws:
            if _norm(kw) in t:
                return canon
    return None


# -----------------------------
# Pays / régions (V1: liste courte + extensible)
# -----------------------------
PAYS = [
    ("tunisie", ["tunisie", "tunis", "république tunisienne", "republique tunisienne"]),
    ("maroc", ["maroc", "royaume du maroc"]),
    ("algérie", ["algérie", "algerie"]),
    ("sénégal", ["sénégal", "senegal"]),
    ("côte d'ivoire", ["côte d'ivoire", "cote d'ivoire", "ivory coast"]),
    ("burkina faso", ["burkina", "burkina faso"]),
    ("niger", ["niger"]),
    ("mali", ["mali"]),
]

REGIONS = [
    ("afrique de l'ouest", ["afrique de l'ouest", "west africa"]),
    ("mena", ["mena", "middle east and north africa", "moyen-orient", "maghreb"]),
    ("afrique du nord", ["afrique du nord", "north africa"]),
]

def detect_pays_region(text: str) -> tuple[Optional[str], Optional[str]]:
    t = _norm(text)
    found_country = None
    for canon, kws in PAYS:
        for kw in kws:
            if _norm(kw) in t:
                found_country = canon
                break
        if found_country:
            break

    found_region = None
    for canon, kws in REGIONS:
        for kw in kws:
            if _norm(kw) in t:
                found_region = canon
                break
        if found_region:
            break

    return found_country, found_region


# -----------------------------
# Dates: publication / deadline
# -----------------------------
DATE_FR_RX = r"\b(\d{1,2})\s+(janv(?:ier)?|févr(?:ier)?|fevr(?:ier)?|mars|avr(?:il)?|mai|juin|juil(?:let)?|août|aout|sept(?:embre)?|oct(?:obre)?|nov(?:embre)?|déc(?:embre)?|dec(?:embre)?)\s+(\d{4})\b"
DATE_SLASH_RX = r"\b(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{4})\b"

DEADLINE_MARKERS = [
    "avant le", "au plus tard", "date limite", "deadline", "soumission", "manifestations d'intérêt doivent",
    "doivent être envoyées", "doivent être envoyes", "before",
]

PUBLICATION_MARKERS = ["publié", "publication", "paru", "date:"]

def _parse_fr_date(match: re.Match) -> Optional[str]:
    d = int(match.group(1))
    mtxt = _norm(match.group(2))
    y = int(match.group(3))
    m = MONTHS_FR.get(mtxt)
    if not m:
        # essayer version tronquée
        m = MONTHS_FR.get(mtxt[:4], None)
    if not m:
        return None
    return _to_iso_date(y, m, d)

def _parse_slash_date(match: re.Match) -> Optional[str]:
    d = int(match.group(1))
    m = int(match.group(2))
    y = int(match.group(3))
    return _to_iso_date(y, m, d)

def _extract_best_date(text: str) -> list[str]:
    dates: list[str] = []
    for m in _find_all(DATE_FR_RX, text):
        iso = _parse_fr_date(m)
        if iso:
            dates.append(iso)
    for m in _find_all(DATE_SLASH_RX, text):
        iso = _parse_slash_date(m)
        if iso:
            dates.append(iso)
    # dédoublonnage en gardant l'ordre
    out = []
    seen = set()
    for d in dates:
        if d not in seen:
            seen.add(d)
            out.append(d)
    return out

def detect_dates(text: str) -> Dict[str, Optional[str]]:
    t = text or ""
    dates = _extract_best_date(t)

    # Heuristique : deadline = date la plus proche d’un marker
    deadline = None
    publication = None

    lower = _norm(t)
    if dates:
        # on essaye d’assigner deadline via window autour de marker
        for marker in DEADLINE_MARKERS:
            pos = lower.find(_norm(marker))
            if pos != -1:
                window = t[pos: min(pos + 600, len(t))]
                dwin = _extract_best_date(window)
                if dwin:
                    deadline = dwin[0]
                    break

        # publication via marker
        for marker in PUBLICATION_MARKERS:
            pos = lower.find(_norm(marker))
            if pos != -1:
                window = t[pos: min(pos + 600, len(t))]
                dwin = _extract_best_date(window)
                if dwin:
                    publication = dwin[0]
                    break

        # fallback simple si rien trouvé
        if not deadline:
            # souvent la dernière date dans un AMI est la date limite
            deadline = dates[-1]
        if not publication:
            # publication souvent la première date (si présente)
            publication = dates[0]

    return {"publication": publication, "deadline": deadline}


# -----------------------------
# Domaine (V1 keywords)
# -----------------------------
DOMAINS = [
    ("informatique / si", ["système d'information", "systeme d'information", "si", "govtech", "digital", "développement", "developpement", "soa", "cloud"]),
    ("amoa / moa", ["amoa", "maîtrise d'ouvrage", "maitrise d'ouvrage", "moa"]),
    ("audit / finance", ["audit", "comptabilité", "comptabilite", "états financiers", "etats financiers", "syscohada", "ohada", "fiscal", "paie"]),
    ("environnement / social", ["environnement", "social", "pg es", "pges", "fies", "sauvegarde", "vbg", "mpr", "mgp"]),
    ("formation", ["formation", "atelier", "renforcement des capacités", "renforcement des capacites"]),
]

def detect_domaine(text: str) -> Optional[str]:
    t = _norm(text)
    for canon, kws in DOMAINS:
        for kw in kws:
            if _norm(kw) in t:
                return canon
    return None


# -----------------------------
# Public API
# -----------------------------
def extract_metadata(text: str) -> Dict[str, Any]:
    """
    Extraction V1 (rules-based).
    Retourne le schéma attendu par ton structured.json.
    """
    lang = detect_language(text)
    bailleur = detect_bailleur(text)
    pays, region = detect_pays_region(text)
    dates = detect_dates(text)
    domaine = detect_domaine(text)

    return {
        "langue": lang,
        "domaine": domaine,
        "bailleur": bailleur,
        "pays": pays,
        "region": region,
        "dates": dates,
    }
