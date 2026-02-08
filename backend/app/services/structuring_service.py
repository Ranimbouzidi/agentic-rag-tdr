import re
from typing import Dict, List, Tuple

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
]


def normalize_text(text: str) -> str:
    if not text:
        return ""

    t = text

    # puces → lignes
    t = t.replace("▪", "\n- ").replace("●", "\n- ").replace("•", "\n- ")

    # réinjecter des retours ligne avant titres fréquents
    t = re.sub(r"(?m)^\s*(I{1,3}\.|IV\.|V\.|VI\.)", r"\n\g<0>", t)
    t = re.sub(r"(?m)^\s*([A-Z]\-)\s*", r"\n\1 ", t)

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



def normalize_for_titles(text: str) -> str:
    # force newline before roman numeral headings (I. II. III. IV. V. etc.)
    text = re.sub(r"(?<!\n)\s*([IVX]{1,6}\.)\s+", r"\n\1 ", text)
    # force newline before A- / B- headings
    text = re.sub(r"(?<!\n)\s*([A-Z])\s*[-–]\s+", r"\n\1- ", text)
    return text


# Mapping : titres possibles -> section canonique
TITLE_TO_SECTION = [
    # Contexte
    (r"\bcontexte\b|\bbackground\b|\bprésentation\b|\bprésentation générale\b", "contexte"),
    (r"contexte|justification|présentation", "contexte"),

    # Mission / objectifs / scope
    (r"\bmission\b|\bobjectifs?\b|\bscope of work\b|\btâches\b|\bdescription de la mission\b|\boffre technique\b|\bprestations?\b|\bdescription\b", "mission"),

    # Livrables
    (r"\blivrables?\b|\bdeliverables?\b|\brésultats attendus\b|\boutputs?\b", "livrables"),

    # Profil / qualifications
    (r"\bprofil\b|\bqualifications\b|\bprofile\b|\bexpérience\b|\brequired qualifications\b", "profil"),

    # Compétences
    (r"\bcompétences\b|\bskills\b|\bexpertise\b", "competences"),
]

TITLE_LINE_REGEXES = [
    r"^\s*[IVX]{1,6}\.\s+.+$",     # I. TITRE
    r"^\s*\d+\.\s+.+$",            # 1. Titre
    r"^\s*[A-Z]\s*[-–]\s+.+$",     # A- Titre
    r"^\s*[A-Z][A-Z\s’'’\-]{6,}$", # LIGNE EN MAJUSCULES
]

def _is_title_line(line: str) -> bool:
    line = line.strip()
    if len(line) < 4:
        return False
    for rx in TITLE_LINE_REGEXES:
        if re.match(rx, line):
            return True
    return False

def _norm(s: str) -> str:
    return re.sub(r"[\s’'’\-–:_]", "", s.lower())

def _title_to_section(title: str) -> str | None:
    t = _norm(title)
    for pattern, section in TITLE_TO_SECTION:
        # on normalise aussi le pattern : on teste sur le titre "normalisé"
        # astuce: on fait un test regex sur le titre original ET sur sa version normalisée
        if re.search(pattern, title.lower()):
            return section
        # fallback: si le pattern est un mot simple, on teste aussi en "collé"
        if _norm(re.sub(r"\\b", "", pattern)) and _norm(re.sub(r"\\b", "", pattern)) in t:
            return section
    return None


def split_into_sections(text: str) -> Dict[str, str]:
    text = normalize_text(text)
    text = normalize_for_titles(text)
    # découpage par titres
    lines = text.splitlines()
    title_spans: List[Tuple[int, str]] = []  # (line_index, title)

    for i, line in enumerate(lines):
        if _is_title_line(line):
            title_spans.append((i, line.strip()))

    # init
    out = {"contexte": "", "mission": "", "livrables": "", "profil": "", "competences": ""}

    if not title_spans:
        # fallback : tout dans mission si aucun titre détecté
        out["mission"] = text.strip()
        out = fill_empty_sections_fallback(text, out)
        return out

    # Parcourir les blocs entre titres
    for idx, (start_i, title) in enumerate(title_spans):
        end_i = title_spans[idx + 1][0] if idx + 1 < len(title_spans) else len(lines)
        block = "\n".join(lines[start_i:end_i]).strip()

        section = _title_to_section(title)
        if section:
            # concat si plusieurs blocs
            if out[section]:
                out[section] += "\n\n" + block
            else:
                out[section] = block

    return out
def _window_extract(text: str, keywords: list[str], window: int = 2500) -> str:
    lower = text.lower()
    positions = []

    for k in keywords:
        k1 = k.lower()
        k2 = k1.replace(" ", "")  # version collée
        p1 = lower.find(k1)
        p2 = lower.replace(" ", "").find(k2)

        if p1 != -1:
            positions.append(p1)
        if p2 != -1:
            # approximation : on prend la position sur texte normal (p2 peut être décalée)
            positions.append(max(p2 - 50, 0))

    if not positions:
        return ""

    start = max(min(positions) - 400, 0)
    end = min(start + window, len(text))
    return text[start:end].strip()


def fill_empty_sections_fallback(text: str, sections: Dict[str, str]) -> Dict[str, str]:
    if not sections.get("mission"):
        sections["mission"] = _window_extract(text, ["mission", "objectifs", "tâches", "prestations", "scope"])
    if not sections.get("livrables"):
        sections["livrables"] = _window_extract(text, ["livrable", "deliverable", "résultats attendus", "outputs"])
    if not sections.get("profil"):
        sections["profil"] = _window_extract(text, ["profil", "qualifications", "expérience", "requirements"])
    if not sections.get("contexte"):
        sections["contexte"] = _window_extract(text, ["contexte", "background", "présentation", "justification"])
    return sections


def extract_competences(text: str, max_items: int = 40) -> List[str]:
    # privilégier les lignes "bullet"
    skills = []
    seen = set()

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue

        # détecte bullet list : "▪", "•", "-", "–"
        if re.match(r"^\s*[▪•\-–]\s+.+", raw):
            item = re.sub(r"^\s*[▪•\-–]\s+", "", raw).strip()
        else:
            continue

        item = re.sub(r"\s+", " ", item).strip()
        if 3 <= len(item) <= 140 and item.lower() not in seen:
            seen.add(item.lower())
            skills.append(item)
        if len(skills) >= max_items:
            break

    return skills
def extract_skills_from_text(text: str) -> list[str]:
    lower = text.lower()
    found = []
    for kw in SKILL_KEYWORDS:
        if kw in lower:
            found.append(kw)
    return sorted(set(found))

def extract_tasks(text: str, max_items: int = 30) -> list[str]:
    tasks = []
    seen = set()

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue

        # bullets longs → tâches
        if re.match(r"^\s*[▪•\-–]\s+.+", raw):
            item = re.sub(r"^\s*[▪•\-–]\s+", "", raw).strip()

            if len(item) > 80 and item.lower() not in seen:
                seen.add(item.lower())
                tasks.append(item)

    return tasks
def procurement_fallback(
    text: str,
    sections: Dict[str, str],
    tasks: list[str],
) -> Dict[str, str]:
    """
    Fallback générique pour les TdR de type appel d'offres (procurement).
    Permet de reconstruire mission / profil / livrables / contexte
    lorsque les sections ne sont pas explicitement structurées.
    """

    # PROFIL : souvent introduit par "l’équipe d’exécution" / "doit comprendre"
    if not sections.get("profil"):
        prof = _window_extract(
    text,
    [
        "L’équipe d’exécution",
        "doit comprendre",
        "Un expert-comptable",
        "bac+5",
        "diplôme supérieur",
        "connaissance solide",
        "expérience",
        "références",
        "profil", "qualification", "compétences requises", "l’équipe", "doit comprendre",
    ],
    window=2200,
)

        if prof:
            sections["profil"] = prof

    # MISSION : dans un TdR procurement, la mission = synthèse des prestations
    mission_txt = (sections.get("mission") or "").lower()

    if (not sections.get("mission")) or ("offre technique" in mission_txt) or ("soumission" in mission_txt):
     if tasks:
        sections["mission"] = (
            "Mission principale : réalisation des prestations attendues décrites dans les termes de référence, "
            "incluant notamment :\n- " + "\n- ".join(tasks[:8])
        )

    # LIVRABLES : d'abord précis, sinon générique
    if not sections.get("livrables"):
        liv = _window_extract(
            text,
            [
                "rapport",
                "états financiers",
                "déclarations fiscales",
                "déclarations sociales",
                "bulletins de paie",
                "declarations",
            ],
            window=1800,
        )
        if not liv:
            liv = _window_extract(
                text,
                ["rapport", "états financiers", "déclarations", "bulletins de paie"]
            )
        if liv:
            sections["livrables"] = liv
    if sections.get("livrables") and len(sections["livrables"]) > 1200:
     sections["livrables"] = (
        "Livrables attendus (synthèse) :\n"
        "- Rapports périodiques (trimestriels ou selon TdR)\n"
        "- Déclarations fiscales et sociales\n"
        "- États financiers annuels et annexes\n"
        "- Documents de paie (si applicable)\n"
    )

    # CONTEXTE : d'abord autour organisation/ONG, sinon fallback début du document
    if not sections.get("contexte"):
        ctx = _window_extract(
            text,
            ["organisation", "ong", "association", "contexte", "présentation", "justification", "introduction", "objet"]
        )
        if ctx:
            sections["contexte"] = ctx
        else:
            sections["contexte"] = text[:2500].strip()

    return sections
