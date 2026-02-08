import re
from typing import Dict, List, Tuple ,Optional

SKILL_KEYWORDS = [
    "ohada",
    "syscohada",
    "audit",
    "contrôle de gestion",
    "comptabilité",
    "fiscalité",
    "paie",
    "états financiers",
    "déclarations fiscales",
    "déclarations sociales",
    "bailleurs",
    "ong",
     "environnement", "social", "sauvegarde", "pgES", "pges", "fies", "mpr", "mgp",
    "banque mondiale", "bird", "vbg",
]


def fix_ocr_spacing(text: str) -> str:
    """
    Heuristiques légères pour corriger les textes OCR où les espaces sont collés.
    Objectif: améliorer le split des sections (CONTEXT/OBJECTIFS/MISSION/LIVRABLES...),
    pas de faire une reconstruction parfaite.
    """
    t = text or ""

    # 1) 1-CONTEXTE -> 1 - CONTEXTE
    t = re.sub(r"(\d)\s*-\s*([A-Z])", r"\1 - \2", t)

    # 2) abcDEF -> abc DEF (inclut lettres accentuées)
    t = re.sub(r"([a-zà-ÿ])([A-ZÀ-ÖØ-Ý])", r"\1 \2", t)

    # 3) lettre+chiffre / chiffre+lettre
    t = re.sub(r"([A-Za-zÀ-ÿ])(\d)", r"\1 \2", t)
    t = re.sub(r"(\d)([A-Za-zÀ-ÿ])", r"\1 \2", t)

    # 4) Collages fréquents en OCR sur apostrophes (optionnel)
    # ex: MINISTEREDEL'AGRICULTURE -> MINISTERE DE L'AGRICULTURE
    t = re.sub(r"\b(DE|DU|DES|DEL|D')(?=[A-ZÀ-ÖØ-Ý])", r"\1 ", t)

    # 5) espaces multiples
    t = re.sub(r"[ \t]{2,}", " ", t)

    return t


def normalize_text(text: str) -> str:
    if not text:
        return ""

    t = text

    # ✅ OCR spacing fix en premier (Cas B)
    t = fix_ocr_spacing(t)

    # puces → lignes
    t = t.replace("▪", "\n- ").replace("●", "\n- ").replace("•", "\n- ")

    # réinjecter des retours ligne avant titres fréquents (roman numerals)
    t = re.sub(r"(?m)^\s*(I{1,3}\.|IV\.|V\.|VI\.)", r"\n\g<0>", t)

    # titres style "A-" en début de ligne
    t = re.sub(r"(?m)^\s*([A-Z]\-)\s*", r"\n\1 ", t)

    # ✅ réduire collages de fin de ligne OCR: "agro-\nsylvo" -> "agrosylvo"
    t = re.sub(r"(\w)-\n(\w)", r"\1\2", t)

    # espaces multiples
    t = re.sub(r"[ \t]+", " ", t)

    # lignes vides multiples
    t = re.sub(r"\n{3,}", "\n\n", t)

    return t.strip()

def clean_and_dedup_tasks(tasks: list[str]) -> list[str]:
    if not tasks:
        return []

    cleaned = []
    seen = set()

    noise_patterns = [
        "envoi des offres",
        "soumission des offres",
        "@",  # emails
        "offre technique",
        "offre financière",
        "critères de sélection",
    ]

    for t in tasks:
        if not t:
            continue
        s = " ".join(t.split()).strip()

        low = s.lower()
        if any(p in low for p in noise_patterns):
            continue

        key = low.replace("’", "'")
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(s)

    return cleaned


# Mapping : titres possibles -> section canonique
TITLE_WORDS = [
    # Contexte
    "CONTEXTE", "JUSTIFICATION", "INTRODUCTION", "PRESENTATION", "PRÉSENTATION",
    # Mission / objectifs
    "OBJECTIF", "OBJECTIFS", "MISSION", "MANDAT", "DESCRIPTION", "PRESTATION", "PRESTATIONS",
    "METHODOLOGIE", "MÉTHODOLOGIE", "APPROCHE",
    "RESULTAT", "RESULTATS", "RÉSULTAT", "RÉSULTATS", "RESULTATS ATTENDUS", "RÉSULTATS ATTENDUS",
    "TACHES", "TÂCHES", "ACTIVITES", "ACTIVITÉS", "RESPONSABILITES", "RESPONSABILITÉS",
    # Livrables
    "LIVRABLES", "DELIVERABLE", "DELIVERABLES", "RAPPORT", "RAPPORTS", "CALENDRIER", "PLANNING",
    # Profil
    "PROFIL", "QUALIFICATIONS", "EXPERIENCE", "EXPÉRIENCE", "CRITERES", "CRITÈRES",
    # Compétences
    "COMPETENCE", "COMPETENCES", "COMPÉTENCE", "COMPÉTENCES", "SKILLS", "EXPERTISE",
]

def normalize_for_titles(text: str) -> str:
    """
    Fusion: ton normalize_for_titles actuel + boost OCR.
    Objectif: aider _is_title_line à détecter les titres même si OCR a collé les espaces.
    """
    t = text or ""

    # (A) Ton existant : roman numerals -> newline
    t = re.sub(r"(?<!\n)\s*([IVX]{1,6}\.)\s+", r"\n\1 ", t)
    # (B) Ton existant : A- / B- -> newline
    t = re.sub(r"(?<!\n)\s*([A-Z])\s*[-–]\s+", r"\n\1- ", t)

    # (C) Boost OCR : "1-CONTEXTE" / "2-OBJECTIFS" -> newline + normalisation "1 - "
    t = re.sub(r"(?<!\n)\s*(\d{1,2})\s*[-–—]\s*([A-ZÀ-ÖØ-Ý])", r"\n\1 - \2", t)

    # (D) Boost OCR : isoler des title words quand ils apparaissent au milieu
    for w in TITLE_WORDS:
        # met un \n avant le mot s’il est précédé par espace (ou début), et suivi de ":" ou espace
        t = re.sub(rf"(?i)(^|\s)({re.escape(w)})(\s|:)", r"\n\2\3", t)

    return t


# -------------------------------------------------------------------
# 2) Détection de ligne "titre" robuste (natif + OCR)
# -------------------------------------------------------------------
TITLE_LINE_REGEXES = [
    r"^\s*[IVX]{1,6}\.\s+.+$",            # I. TITRE
    r"^\s*\d+\.\s+.+$",                   # 1. Titre
    r"^\s*\d+\s*[-–—]\s*.+$",             # 1 - TITRE (OCR-friendly)
    r"^\s*[A-Z]\s*[-–]\s+.+$",            # A- Titre
    r"^\s*[A-Z][A-Z\s’'’\-\–—:]{6,}$",    # LIGNE EN MAJUSCULES
    r"^\s*(CONTEXTE|OBJECTIF|OBJECTIFS|MISSION|LIVRABLES|PROFIL|QUALIFICATIONS|COMPETENCES|TACHES|ACTIVITES|METHODOLOGIE|RESULTATS)\b.*$",
]

def _is_title_line(line: str) -> bool:
    s = (line or "").strip()
    if len(s) < 4:
        return False
    # trop long => rarement un titre (OCR peut produire des lignes longues)
    if len(s) > 140:
        return False
    for rx in TITLE_LINE_REGEXES:
        if re.match(rx, s, flags=re.IGNORECASE):
            return True

    # fallback OCR : ratio de majuscules élevé
    letters = re.sub(r"[^A-Za-zÀ-ÿ]", "", s)
    if len(letters) >= 8:
        upp = re.sub(r"[^A-ZÀ-ÖØ-Ý]", "", s)
        ratio = len(upp) / max(1, len(letters))
        if ratio >= 0.75 and len(s.split()) <= 14:
            return True

    return False


# -------------------------------------------------------------------
# 3) Mapping titre -> section (FR/EN + OCR-friendly)
# -------------------------------------------------------------------
TITLE_TO_SECTION = [
    # Contexte
    (r"\bcontexte\b|\bbackground\b|\bjustification\b|\bprésentation\b|\bintroduction\b", "contexte"),

    # Mission / objectifs / scope (on y met aussi activités/tâches)
    (r"\bmission\b|\bobjectifs?\b|\bscope of work\b|\bterms of reference\b|\btâches\b|\btasks\b|\bactivit", "mission"),
    (r"\bm[eé]thodolog", "mission"),
    (r"\br[eé]sultat", "mission"),
    (r"\bdescription\b|\bprestations?\b|\bmandat\b", "mission"),

    # Livrables
    (r"\blivrables?\b|\bdeliverables?\b|\boutputs?\b|\brapports?\b|\bplanning\b|\bcalendrier\b", "livrables"),

    # Profil / qualifications
    (r"\bprofil\b|\bqualifications?\b|\bprofile\b|\bexp[eé]rience\b|\brequired qualifications\b|\bcriteria\b|\bcrit[eè]res\b", "profil"),

    # Compétences
    (r"\bcomp[eé]tences?\b|\bskills\b|\bexpertise\b", "competences"),
]

def _title_to_section(title: str) -> Optional[str]:
    s = (title or "").strip().lower()
    if not s:
        return None

    # OCR-friendly: supprimer espaces/tirets ponctuation pour matcher "OBJECTIFSDUPROJET"
    compact = re.sub(r"[\s’'’\-\–—:_]", "", s)

    for pattern, section in TITLE_TO_SECTION:
        if re.search(pattern, s, flags=re.IGNORECASE):
            return section

        # fallback "compact" : ex "objectifsduprojet" contient "objectifs"
        # on récupère un token simple du pattern si possible
        token = re.sub(r"\\b|\(|\)|\?|\*|\+|\||\.", "", pattern)
        token = token.split("|")[0]  # premier terme
        token_compact = re.sub(r"[\s’'’\-\–—:_]", "", token.lower())
        if token_compact and token_compact in compact:
            return section

    return None


# -------------------------------------------------------------------
# 4) Window fallback (tu l’as déjà) + fill empties
# -------------------------------------------------------------------
def _window_extract(text: str, keywords: list[str], window: int = 2500) -> str:
    lower = (text or "").lower()
    positions = []

    compact = lower.replace(" ", "")
    for k in keywords:
        k1 = k.lower()
        k2 = k1.replace(" ", "")
        p1 = lower.find(k1)
        p2 = compact.find(k2)

        if p1 != -1:
            positions.append(p1)
        if p2 != -1:
            positions.append(max(p2 - 50, 0))

    if not positions:
        return ""

    start = max(min(positions) - 400, 0)
    end = min(start + window, len(text))
    return (text[start:end] or "").strip()


def fill_empty_sections_fallback(text: str, sections: Dict[str, str]) -> Dict[str, str]:
    # on cherche aussi versions "collées" grâce à _window_extract
    if not sections.get("mission"):
        sections["mission"] = _window_extract(
            text,
            ["mission", "objectifs", "tâches", "taches", "activités", "activites", "prestations", "scope", "méthodologie", "methodologie", "résultats", "resultats"],
        )
    if not sections.get("livrables"):
        sections["livrables"] = _window_extract(
            text,
            ["livrable", "deliverable", "résultats attendus", "resultats attendus", "outputs", "rapport", "planning", "calendrier"],
        )
    if not sections.get("profil"):
        sections["profil"] = _window_extract(
            text,
            ["profil", "qualifications", "expérience", "experience", "requirements", "criteria", "critères", "criteres"],
        )
    if not sections.get("contexte"):
        sections["contexte"] = _window_extract(
            text,
            ["contexte", "background", "présentation", "presentation", "justification", "introduction"],
        )
    return sections


# -------------------------------------------------------------------
# 5) Split (conserve ton comportement + améliore OCR)
# -------------------------------------------------------------------
def split_into_sections(text: str) -> Dict[str, str]:
    text = normalize_text(text)
    text = normalize_for_titles(text)

    lines = text.splitlines()
    title_spans: List[Tuple[int, str]] = []

    for i, line in enumerate(lines):
        if _is_title_line(line):
            title_spans.append((i, line.strip()))

    out = {"contexte": "", "mission": "", "livrables": "", "profil": "", "competences": ""}

    if not title_spans:
        out["mission"] = text.strip()
        return fill_empty_sections_fallback(text, out)

    for idx, (start_i, title) in enumerate(title_spans):
        end_i = title_spans[idx + 1][0] if idx + 1 < len(title_spans) else len(lines)
        block = "\n".join(lines[start_i:end_i]).strip()

        section = _title_to_section(title)
        if section:
            if out[section]:
                out[section] += "\n\n" + block
            else:
                out[section] = block

    # ✅ Important : si OCR a loupé certaines sections, on complète
    out = fill_empty_sections_fallback(text, out)
    return out
def extract_skills_from_text(text: str) -> list[str]:
    lower = (text or "").lower()
    found = []
    for kw in SKILL_KEYWORDS:
        if kw.lower() in lower:
            found.append(kw.lower())
    return sorted(set(found))

def extract_competences(text: str, max_items: int = 40) -> list[str]:
    """
    Backward compatible wrapper.
    Ancien import utilisé par structuring_process_service.py.
    Si tu utilises déjà extract_skills_from_text() ailleurs, ça ne gêne pas.
    """
    return extract_skills_from_text(text)[:max_items]

def extract_tasks(text: str, max_items: int = 30) -> list[str]:
    """
    Extraction des tâches.
    - Natif: bullets (-, •, ▪) => OK
    - OCR: parfois pas de bullets => on récupère aussi les lignes longues finissant par ; ou .
    """
    if not text:
        return []

    tasks: list[str] = []
    seen = set()

    lines = (text or "").splitlines()
    for raw in lines:
        line = (raw or "").strip()
        if not line:
            continue

        # 1) Bullet tasks (natif + OCR)
        if re.match(r"^\s*[▪•\-–]\s+.+", raw):
            item = re.sub(r"^\s*[▪•\-–]\s+", "", raw).strip()
            item = re.sub(r"\s+", " ", item).strip()
            if len(item) >= 25:
                k = item.lower()
                if k not in seen:
                    seen.add(k)
                    tasks.append(item)
            continue

        # 2) OCR style: liste séparée par ';' ou lignes longues type "La mission consiste à..."
        # On prend les lignes "actionables" (commencent par verbe / "Assurer", "Réaliser", "Mettre en place"...)
        if len(line) >= 60 and (line.endswith(";") or line.endswith(".") or line.endswith(":")):
            if re.match(r"(?i)^(assurer|réaliser|realiser|mettre|appuyer|participer|élaborer|elaborer|produire|préparer|preparer|organiser|conduire|suivre|analyser|contrôler|controler|former|sensibiliser)\b", line):
                item = re.sub(r"\s+", " ", line).strip()
                k = item.lower()
                if k not in seen:
                    seen.add(k)
                    tasks.append(item)

        if len(tasks) >= max_items:
            break

    return tasks[:max_items]


def procurement_fallback(text: str, sections: Dict[str, str], tasks: list[str]) -> Dict[str, str]:
    """
    Ton fallback procurement (appel d'offres) — version safe.
    IMPORTANT: On le gardera, mais il faut l'appeler seulement si markers procurement.
    """
    # PROFIL
    if not sections.get("profil"):
        prof = _window_extract(
            text,
            [
                "l’équipe d’exécution", "l'equipe d'execution", "doit comprendre", "profil",
                "qualification", "compétences requises", "competences requises",
                "expérience", "experience", "références", "references"
            ],
            window=2200,
        )
        if prof:
            sections["profil"] = prof

    # MISSION
    mission_txt = (sections.get("mission") or "").lower()
    if (not sections.get("mission")) or ("offre technique" in mission_txt) or ("soumission" in mission_txt):
        if tasks:
            sections["mission"] = (
                "Mission principale : réalisation des prestations attendues décrites dans les termes de référence, "
                "incluant notamment :\n- " + "\n- ".join(tasks[:8])
            )

    # LIVRABLES
    if not sections.get("livrables"):
        liv = _window_extract(
            text,
            ["livrable", "deliverable", "rapport", "rapports", "planning", "calendrier", "outputs"],
            window=1800,
        )
        if liv:
            sections["livrables"] = liv

    # CONTEXTE
    if not sections.get("contexte"):
        ctx = _window_extract(
            text,
            ["contexte", "introduction", "justification", "présentation", "presentation", "objet", "organisation"],
            window=2500,
        )
        sections["contexte"] = ctx if ctx else (text[:2500].strip())

    return sections


__all__ = [
    "normalize_text",
    "split_into_sections",
    "extract_tasks",
    "extract_competences",
    "extract_skills_from_text",
    "procurement_fallback",
    "clean_and_dedup_tasks",
]

