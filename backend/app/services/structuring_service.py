# backend/app/services/structuring_service.py
from __future__ import annotations

import re
from typing import Dict, List, Tuple, Optional, Any

from app.services.tracing import span_step


# -------------------------------------------------------------------
# Keywords métiers (compétences)
# -------------------------------------------------------------------
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
    "environnement", "social", "sauvegarde", "pges", "fies", "mpr", "mgp",
    "banque mondiale", "bird", "vbg",
]


# -------------------------------------------------------------------
# OCR / normalisation
# -------------------------------------------------------------------
def fix_ocr_spacing(text: str) -> str:
    """
    Heuristiques légères pour corriger les textes OCR où les espaces sont collés.
    Objectif: améliorer la détection des titres/sections, pas reconstruire le texte parfaitement.
    """
    with span_step("structure.fix_ocr_spacing", in_len=len(text or "")):
        t = text or ""

        t = re.sub(r"(\d)\s*-\s*([A-Z])", r"\1 - \2", t)
        t = re.sub(r"([a-zà-ÿ])([A-ZÀ-ÖØ-Ý])", r"\1 \2", t)
        t = re.sub(r"([A-Za-zÀ-ÿ])(\d)", r"\1 \2", t)
        t = re.sub(r"(\d)([A-Za-zÀ-ÿ])", r"\1 \2", t)
        t = re.sub(r"\b(DE|DU|DES|DEL|D')(?=[A-ZÀ-ÖØ-Ý])", r"\1 ", t)
        t = re.sub(r"[ \t]{2,}", " ", t)

        return t


def normalize_text(text: str) -> str:
    if not text:
        return ""

    with span_step("structure.normalize_text", in_len=len(text or "")) as span:
        t = text

        t = fix_ocr_spacing(t)
        t = t.replace("▪", "\n- ").replace("●", "\n- ").replace("•", "\n- ")
        t = re.sub(r"(?m)^\s*(I{1,3}\.|IV\.|V\.|VI\.)", r"\n\g<0>", t)
        t = re.sub(r"(?m)^\s*([A-Z]\-)\s*", r"\n\1 ", t)
        t = re.sub(r"(\w)-\n(\w)", r"\1\2", t)
        t = re.sub(r"[ \t]+", " ", t)
        t = re.sub(r"\n{3,}", "\n\n", t)

        out = t.strip()
        span.set_attribute("out_len", len(out))
        return out


def clean_and_dedup_tasks(tasks: List[str]) -> List[str]:
    if not tasks:
        return []

    with span_step("structure.clean_and_dedup_tasks", in_count=len(tasks)) as span:
        cleaned: List[str] = []
        seen = set()

        noise_patterns = [
            "envoi des offres",
            "soumission des offres",
            "@",
            "offre technique",
            "offre financière",
            "critères de sélection",
            "critères d'évaluation",
            "proposition financière",
            "proposition technique",
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

        span.set_attribute("out_count", len(cleaned))
        return cleaned


# -------------------------------------------------------------------
# Titres / sections
# -------------------------------------------------------------------
TITLE_WORDS = [
    "CONTEXTE", "JUSTIFICATION", "INTRODUCTION", "PRESENTATION", "PRÉSENTATION",
    "OBJECTIF", "OBJECTIFS", "MISSION", "MANDAT", "DESCRIPTION", "PRESTATION", "PRESTATIONS",
    "METHODOLOGIE", "MÉTHODOLOGIE", "APPROCHE",
    "RESULTAT", "RESULTATS", "RÉSULTAT", "RÉSULTATS", "RESULTATS ATTENDUS", "RÉSULTATS ATTENDUS",
    "TACHES", "TÂCHES", "ACTIVITES", "ACTIVITÉS", "RESPONSABILITES", "RESPONSABILITÉS",
    "LIVRABLES", "DELIVERABLE", "DELIVERABLES", "RAPPORT", "RAPPORTS", "CALENDRIER", "PLANNING",
    "PROFIL", "QUALIFICATIONS", "EXPERIENCE", "EXPÉRIENCE", "CRITERES", "CRITÈRES",
    "COMPETENCE", "COMPETENCES", "COMPÉTENCE", "COMPÉTENCES", "SKILLS", "EXPERTISE",
]


def normalize_for_titles(text: str) -> str:
    with span_step("structure.normalize_for_titles", in_len=len(text or "")) as span:
        t = text or ""
        t = re.sub(r"(?<!\n)\s*([IVX]{1,6}\.)\s+", r"\n\1 ", t)
        t = re.sub(r"(?<!\n)\s*([A-Z])\s*[-–]\s+", r"\n\1- ", t)
        t = re.sub(r"(?<!\n)\s*(\d{1,2})\s*[-–—]\s*([A-ZÀ-ÖØ-Ý])", r"\n\1 - \2", t)

        for w in TITLE_WORDS:
            t = re.sub(rf"(?i)(^|\s)({re.escape(w)})(\s|:)", r"\n\2\3", t)

        span.set_attribute("out_len", len(t))
        return t


TITLE_LINE_REGEXES = [
    r"^\s*[IVX]{1,6}\.\s+.+$",
    r"^\s*\d+\.\s+.+$",
    r"^\s*\d+\s*[-–—]\s*.+$",
    r"^\s*[A-Z]\s*[-–]\s+.+$",
    r"^\s*[A-Z][A-Z\s’'’\-\–—:]{6,}$",
    r"^\s*(CONTEXTE|OBJECTIF|OBJECTIFS|MISSION|LIVRABLES|PROFIL|QUALIFICATIONS|COMPETENCES|TACHES|ACTIVITES|METHODOLOGIE|RESULTATS)\b.*$",
]


def _is_title_line(line: str) -> bool:
    s = (line or "").strip()
    if len(s) < 4:
        return False
    if len(s) > 140:
        return False

    if len(s.split()) > 15:
        if not re.match(r"^\s*([IVX]{1,6}\.|(\d+(\.|[-–—]))|[A-Z]\s*[-–])\s+", s):
            return False

    for rx in TITLE_LINE_REGEXES:
        if re.match(rx, s, flags=re.IGNORECASE):
            return True

    letters = re.sub(r"[^A-Za-zÀ-ÿ]", "", s)
    if len(letters) >= 8:
        upp = re.sub(r"[^A-ZÀ-ÖØ-Ý]", "", s)
        ratio = len(upp) / max(1, len(letters))
        if ratio >= 0.75 and len(s.split()) <= 14:
            return True

    return False


TITLE_TO_SECTION = [
    (r"\b(dossier\s+(à|a)\s+soumettre|pi[eè]ces?\s+(à|a)\s+fournir|modalit[eé]s?\s+de\s+soumission|soumission|candidature|postuler|comment\s+postuler|adresse\s+e-?mail|courrier\s+[eé]lectronique|deadline|date\s+limite|dernier\s+d[eé]lai|date\s+de\s+cl[oô]ture|contact|contacts)\b", "candidature"),
    (r"\b(crit[eè]res?\s+d[’']?[eé]valuation|grille\s+d[’']?[eé]valuation|m[eé]thode\s+d[’']?[eé]valuation|modalit[eé]s?\s+d[’']?[eé]valuation|notation|bar[eè]me|score|pond[eé]ration|s[eé]lection|processus\s+de\s+s[eé]lection|analyse\s+des\s+dossiers|proposition\s+technique|proposition\s+financi[eè]re|offre\s+technique|offre\s+financi[eè]re)\b", "evaluation"),
    (r"\b(tâches?|taches?|tasks?|activit[eé]s?|activities?|responsabilit[eé]s?|responsibilities?|r[oô]les?|role|scope\s+des\s+activit[eé]s?)\b", "taches"),
    (r"\b(contexte|background|justification|pr[eé]sentation|presentation|introduction|cadre\s+g[eé]n[eé]ral|contexte\s+g[eé]n[eé]ral)\b", "contexte"),
    (r"\b(livrables?|deliverables?|outputs?|produits?\s+attendus?|r[eé]sultats?\s+attendus?|expected\s+results?|documents?\s+(à|a)\s+remettre|remise\s+des\s+livrables?)\b", "livrables"),
    (r"\b(planning|calendrier|timeline|chronogramme|plan\s+de\s+travail|work\s+plan)\b", "planning"),
    (r"\b(profil|profile|qualifications?|comp[eé]tences?\s+requises?|exp[eé]rience|experience|pr[eé]requis|prerequis|formation|dipl[oô]me|expertise\s+requise|requirements?|required\s+qualifications?)\b", "profil"),
    (r"\b(comp[eé]tences?|skills?|mots[-\s]?cl[eé]s?|expertise)\b", "competences"),
    (r"\b(mission|objectifs?|objective|objectives|description|prestations?|mandat|scope\s+of\s+work|terms?\s+of\s+reference|termes?\s+de\s+r[eé]f[eé]rence)\b", "mission"),
    (r"\b(m[eé]thodolog\w*|approche|methodology|approach)\b", "mission"),
    (r"\b(r[eé]sultat\w*|outcomes?)\b", "mission"),
]


def _title_to_section(title: str) -> Optional[str]:
    s = (title or "").strip().lower()
    if not s:
        return None

    compact = re.sub(r"[\s’'’\-\–—:_]", "", s)

    for pattern, section in TITLE_TO_SECTION:
        if re.search(pattern, s, flags=re.IGNORECASE):
            return section

        token = re.sub(r"\\b|\(|\)|\?|\*|\+|\||\.", "", pattern)
        token = token.split("|")[0]
        token_compact = re.sub(r"[\s’'’\-\–—:_]", "", token.lower())
        if token_compact and token_compact in compact:
            return section

    return None


# -------------------------------------------------------------------
# Window fallback scoring
# -------------------------------------------------------------------
def _window_extract(text: str, keywords: List[str], window: int = 2500) -> str:
    lower = (text or "").lower()
    positions: List[int] = []

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


def _score_window(window_text: str, include: List[str], exclude: List[str]) -> int:
    if not window_text:
        return -10_000
    low = window_text.lower()

    inc = 0
    for k in include:
        k1 = k.lower()
        if k1 in low or k1.replace(" ", "") in low.replace(" ", ""):
            inc += 2

    exc = 0
    for k in exclude:
        k1 = k.lower()
        if k1 in low or k1.replace(" ", "") in low.replace(" ", ""):
            exc += 3

    bonus = 0
    if re.search(r"(?m)^\s*[-•▪]\s+\S+", window_text):
        bonus += 2

    if re.search(r"(?i)\b(offre\s+technique|offre\s+financi|proposition\s+technique|proposition\s+financi|notation|bar[eè]me|pond[eé]ration)\b", window_text):
        bonus += 1

    if len(window_text.strip()) < 120:
        bonus -= 2

    return inc + bonus - exc


def _best_window(text: str, include: List[str], exclude: Optional[List[str]] = None, window: int = 2500) -> str:
    exclude = exclude or []
    candidates: List[str] = []

    for k in include[:12]:
        w = _window_extract(text, [k], window=window)
        if w:
            candidates.append(w)

    if not candidates:
        w = _window_extract(text, include, window=window)
        return w or ""

    best = ""
    best_score = -10_000
    for c in candidates:
        sc = _score_window(c, include=include, exclude=exclude)
        if sc > best_score:
            best_score = sc
            best = c
    return best


def fill_empty_sections_fallback(text: str, sections: Dict[str, str]) -> Dict[str, str]:
    t = text or ""
    if not t.strip():
        return sections

    with span_step("structure.fill_empty_sections_fallback"):
        KW = {
            "contexte": ["contexte", "background", "justification", "introduction", "présentation", "presentation", "cadre", "contexte général"],
            "mission": ["mission", "objectifs", "objectif", "description", "prestations", "mandat", "scope of work", "terms of reference", "termes de référence", "méthodologie", "methodologie", "approche", "résultats", "resultats"],
            "taches": ["tâches", "taches", "tasks", "activités", "activites", "activities", "responsabilités", "responsabilites", "rôles", "roles"],
            "livrables": ["livrables", "livrable", "deliverable", "deliverables", "outputs", "produits attendus", "documents à remettre", "remise des livrables", "rapport final", "rapport analytique", "policy brief", "feuille de route"],
            "planning": ["planning", "calendrier", "timeline", "chronogramme", "durée", "duree", "date", "dates", "délai", "delai"],
            "profil": ["profil", "qualifications", "qualification", "profile", "expérience", "experience", "formation", "diplôme", "diplome", "compétences requises", "competences requises", "prérequis", "prerequis", "requirements", "required qualifications"],
            "competences": ["compétences", "competences", "skills", "expertise", "mots-clés", "mots cles"],
            "evaluation": ["critères d'évaluation", "critères", "criteres", "grille d'évaluation", "notation", "barème", "bareme", "pondération", "ponderation", "proposition technique", "proposition financière", "proposition financiere", "offre technique", "offre financière", "offre financiere", "sélection", "selection", "analyse des dossiers"],
            "candidature": ["dossier à soumettre", "dossier a soumettre", "pièces à fournir", "pieces a fournir", "soumission", "candidature", "postuler", "modalités de soumission", "adresse", "email", "e-mail", "courrier électronique", "courrier electronique", "dernier délai", "dernier delai", "date limite", "deadline", "contact"],
        }

        EX = {
            "livrables": ["notation", "barème", "bareme", "pondération", "ponderation", "proposition financière", "offre financière", "critères d'évaluation"],
            "profil": ["proposition financière", "offre financière", "pondération", "barème", "notation"],
            "mission": ["dossier à soumettre", "soumission", "postuler", "adresse e-mail", "deadline", "date limite"],
            "planning": ["pondération", "barème", "notation", "proposition technique", "proposition financière"],
        }

        if not (sections.get("contexte") or "").strip():
            sections["contexte"] = _best_window(t, KW["contexte"], window=2600)

        if not (sections.get("candidature") or "").strip():
            sections["candidature"] = _best_window(t, KW["candidature"], window=2400)

        if not (sections.get("evaluation") or "").strip():
            sections["evaluation"] = _best_window(t, KW["evaluation"], window=2600)

        if not (sections.get("livrables") or "").strip():
            sections["livrables"] = _best_window(t, KW["livrables"], exclude=EX["livrables"], window=3000)

        if not (sections.get("planning") or "").strip():
            sections["planning"] = _best_window(t, KW["planning"], exclude=EX["planning"], window=2400)

        if not (sections.get("profil") or "").strip():
            sections["profil"] = _best_window(t, KW["profil"], exclude=EX["profil"], window=2800)

        if not (sections.get("taches") or "").strip():
            sections["taches"] = _best_window(t, KW["taches"], window=2800)

        if not (sections.get("mission") or "").strip():
            sections["mission"] = _best_window(t, KW["mission"], exclude=EX["mission"], window=3200)

        if not (sections.get("competences") or "").strip():
            sections["competences"] = _best_window(t, KW["competences"], window=1800)

        return sections


# -------------------------------------------------------------------
# Split par titres (avec sections enrichies)
# -------------------------------------------------------------------
def split_into_sections(text: str) -> Dict[str, str]:
    with span_step("structure.split_into_sections", in_len=len(text or "")) as span:
        text = normalize_text(text)
        text = normalize_for_titles(text)

        lines = text.splitlines()
        title_spans: List[Tuple[int, str]] = []

        for i, line in enumerate(lines):
            if _is_title_line(line):
                title_spans.append((i, line.strip()))

        out: Dict[str, str] = {
            "contexte": "",
            "mission": "",
            "taches": "",
            "livrables": "",
            "planning": "",
            "profil": "",
            "competences": "",
            "evaluation": "",
            "candidature": "",
            "taches_table": "",
        }

        if not title_spans:
            out["mission"] = text.strip()
            out2 = fill_empty_sections_fallback(text, out)
            span.set_attribute("titles.count", 0)
            return out2

        span.set_attribute("titles.count", len(title_spans))

        for idx, (start_i, title) in enumerate(title_spans):
            end_i = title_spans[idx + 1][0] if idx + 1 < len(title_spans) else len(lines)
            block = "\n".join(lines[start_i + 1:end_i]).strip()

            section = _title_to_section(title)
            if section:
                if out.get(section):
                    out[section] = (out[section] + "\n\n" + block).strip()
                else:
                    out[section] = block

        out = fill_empty_sections_fallback(text, out)
        return out


# -------------------------------------------------------------------
# Extraction compétences
# -------------------------------------------------------------------
def extract_skills_from_text(text: str) -> List[str]:
    with span_step("structure.extract_skills", in_len=len(text or "")) as span:
        lower = (text or "").lower()
        found: List[str] = []
        for kw in SKILL_KEYWORDS:
            if kw.lower() in lower:
                found.append(kw.lower())

        out = sorted(set(found))
        span.set_attribute("skills.count", len(out))
        return out


def extract_competences(text: str, max_items: int = 40) -> List[str]:
    return extract_skills_from_text(text)[:max_items]


# -------------------------------------------------------------------
# Extraction tâches
# -------------------------------------------------------------------
def extract_tasks(text: str, max_items: int = 30) -> List[str]:
    if not text:
        return []

    with span_step("structure.extract_tasks", in_len=len(text or ""), max_items=max_items) as span:
        tasks: List[str] = []
        seen = set()

        lines = (text or "").splitlines()
        for raw in lines:
            line = (raw or "").strip()
            if not line:
                continue

            if re.match(r"^\s*[▪•\-–]\s+.+", raw):
                item = re.sub(r"^\s*[▪•\-–]\s+", "", raw).strip()
                item = re.sub(r"\s+", " ", item).strip()
                if len(item) >= 25:
                    k = item.lower()
                    if k not in seen:
                        seen.add(k)
                        tasks.append(item)
                continue

            if len(line) >= 60 and (line.endswith(";") or line.endswith(".") or line.endswith(":")):
                if re.match(r"(?i)^(assurer|réaliser|realiser|mettre|appuyer|participer|élaborer|elaborer|produire|préparer|preparer|organiser|conduire|suivre|analyser|contrôler|controler|former|sensibiliser)\b", line):
                    item = re.sub(r"\s+", " ", line).strip()
                    k = item.lower()
                    if k not in seen:
                        seen.add(k)
                        tasks.append(item)

            if len(tasks) >= max_items:
                break

        out = clean_and_dedup_tasks(tasks[:max_items])
        span.set_attribute("tasks.count", len(out))
        return out


# -------------------------------------------------------------------
# Procurement fallback (conservé)
# -------------------------------------------------------------------
def procurement_fallback(text: str, sections: Dict[str, str], tasks: List[str]) -> Dict[str, str]:
    with span_step("structure.procurement_fallback"):
        if not (sections.get("profil") or "").strip():
            prof = _window_extract(
                text,
                [
                    "l’équipe d’exécution", "l'equipe d'execution", "doit comprendre", "profil",
                    "qualification", "compétences requises", "competences requises",
                    "expérience", "experience", "références", "references",
                ],
                window=2200,
            )
            if prof:
                sections["profil"] = prof

        mission_txt = (sections.get("mission") or "").lower()
        if (not (sections.get("mission") or "").strip()) or ("offre technique" in mission_txt) or ("soumission" in mission_txt):
            if tasks:
                sections["mission"] = (
                    "Mission principale : réalisation des prestations attendues décrites dans les termes de référence, "
                    "incluant notamment :\n- " + "\n- ".join(tasks[:8])
                )

        if not (sections.get("livrables") or "").strip():
            liv = _window_extract(
                text,
                ["livrable", "deliverable", "rapport", "rapports", "planning", "calendrier", "outputs"],
                window=1800,
            )
            if liv:
                sections["livrables"] = liv

        if not (sections.get("contexte") or "").strip():
            ctx = _window_extract(
                text,
                ["contexte", "introduction", "justification", "présentation", "presentation", "objet", "organisation"],
                window=2500,
            )
            sections["contexte"] = ctx if ctx else (text[:2500].strip())

        return sections


# -------------------------------------------------------------------
# Markdown tables enrichment
# -------------------------------------------------------------------
def extract_markdown_tables(md: str) -> List[Dict[str, Any]]:
    tables: List[Dict[str, Any]] = []
    if not md:
        return tables

    with span_step("structure.extract_markdown_tables", md_len=len(md or "")) as span:
        lines = md.splitlines()
        i = 0
        while i < len(lines) - 2:
            header = lines[i].strip()
            sep = lines[i + 1].strip()

            is_table_header = ("|" in header) and ("|" in sep) and bool(re.search(r"-{3,}", sep))
            if not is_table_header:
                i += 1
                continue

            headers = [h.strip() for h in header.split("|") if h.strip()]
            i += 2

            rows: List[Dict[str, str]] = []
            while i < len(lines):
                row_line = lines[i].strip()
                if "|" not in row_line:
                    break
                cells = [c.strip() for c in row_line.split("|") if c.strip()]
                if headers and len(cells) == len(headers):
                    rows.append(dict(zip(headers, cells)))
                i += 1

            if headers and rows:
                tables.append({"headers": headers, "rows": rows})

        span.set_attribute("tables.count", len(tables))
        return tables


def _norm_header(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[’'`]", "'", s)
    s = re.sub(r"[^a-z0-9à-ÿ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _table_signature(headers: List[str]) -> str:
    return " ".join(_norm_header(h) for h in headers)


def _table_to_bullets(rows: List[Dict[str, str]], max_items: int = 12) -> str:
    out: List[str] = []
    for r in rows[:max_items]:
        parts: List[str] = []
        for k, v in r.items():
            v = (v or "").strip()
            if not v:
                continue
            kk = (k or "").strip()
            parts.append(f"{kk}: {v}")
        if parts:
            out.append("- " + " | ".join(parts))
    return "\n".join(out).strip()


def _append_section(sections: Dict[str, str], key: str, title: str, bullets: str, max_chars: int = 4000) -> None:
    if not bullets:
        return
    chunk = f"{title}\n{bullets}".strip()

    current = (sections.get(key) or "").strip()
    if not current:
        sections[key] = chunk[:max_chars]
        return

    if bullets in current:
        return

    merged = (current + "\n\n" + chunk).strip()
    sections[key] = merged[:max_chars]


def enrich_sections_from_markdown_tables(sections: Dict[str, str], markdown: str) -> Dict[str, str]:
    if not markdown or not markdown.strip():
        return sections

    with span_step("structure.enrich_from_markdown_tables", md_len=len(markdown or "")) as span:
        tables = extract_markdown_tables(markdown)
        if not tables:
            span.set_attribute("tables.count", 0)
            return sections

        EVAL_KEYS = [
            "critère", "critere", "évaluation", "evaluation", "grille", "notation",
            "score", "barème", "bareme", "pondération", "ponderation", "%", "proposition technique",
            "proposition financière", "proposition financiere", "offre technique", "offre financière",
            "offre financiere", "sélection", "selection",
        ]

        CANDIDATURE_KEYS = [
            "dossier", "soumettre", "soumission", "candidature", "postuler",
            "email", "e-mail", "courrier", "adresse", "contact",
            "deadline", "date limite", "dernier délai", "dernier delai",
        ]

        LIVRABLE_KEYS = ["livrable", "deliverable", "output", "rapport", "report", "document", "remise"]
        PLANNING_KEYS = ["planning", "calendrier", "timeline", "date", "délai", "delai", "durée", "duree", "jours", "mois"]
        PROFIL_KEYS = ["profil", "profile", "qualification", "qualifications", "expérience", "experience", "diplôme", "diplome", "rôle", "role", "poste", "position"]
        TASK_KEYS = ["tâche", "tache", "task", "activité", "activite", "activity", "responsabilité", "responsabilite", "description", "scope"]

        sections.setdefault("taches_table", "")

        for t in tables:
            headers = t.get("headers", []) or []
            rows = t.get("rows", []) or []
            if not headers or not rows:
                continue

            sig = _table_signature(headers)
            bullets = _table_to_bullets(rows)
            if not bullets:
                continue

            if any(k in sig for k in EVAL_KEYS):
                _append_section(sections, "evaluation", "Critères d'évaluation (extraits de tableaux) :", bullets)
                continue

            if any(k in sig for k in CANDIDATURE_KEYS):
                _append_section(sections, "candidature", "Candidature / Soumission (extraits de tableaux) :", bullets)
                continue

            is_livrables = any(k in sig for k in LIVRABLE_KEYS)
            is_planning = any(k in sig for k in PLANNING_KEYS)

            if is_livrables:
                _append_section(sections, "livrables", "Livrables (extraits de tableaux) :", bullets)

            if is_planning:
                _append_section(sections, "planning", "Planning (extraits de tableaux) :", bullets)

            if any(k in sig for k in PROFIL_KEYS):
                _append_section(sections, "profil", "Profil / Qualifications (extraits de tableaux) :", bullets)

            if any(k in sig for k in TASK_KEYS):
                _append_section(sections, "mission", "Activités / Tâches (extraits de tableaux) :", bullets)
                _append_section(sections, "taches_table", "Tâches (tableau) :", bullets)

        return sections


__all__ = [
    "normalize_text",
    "split_into_sections",
    "extract_tasks",
    "extract_competences",
    "extract_skills_from_text",
    "procurement_fallback",
    "clean_and_dedup_tasks",
    "extract_markdown_tables",
    "enrich_sections_from_markdown_tables",
]
