# backend/app/services/filters_utils.py
from __future__ import annotations
from typing import Any, Dict, Optional
import re

_SECTION_ALIASES = {
    # canon -> aliases
    "contexte": {"contexte", "context", "background", "introduction", "presentation", "présentation", "justification"},
    "mission": {"mission", "objectifs", "objectif", "scope", "scope of work", "terms of reference", "termes de reference", "termes de référence", "description"},
    "livrables": {"livrables", "livrable", "deliverable", "deliverables", "outputs", "resultats attendus", "résultats attendus"},
    "planning": {"planning", "calendrier", "timeline", "chronogramme", "plan de travail", "work plan"},
    "profil": {"profil", "profile", "qualifications", "qualification", "experience", "expérience", "requirements"},
    "competences": {"competences", "compétences", "skills", "expertise"},
    "taches": {"taches", "tâches", "tasks", "activites", "activités", "responsabilites", "responsabilités"},
    "evaluation": {"evaluation", "évaluation", "criteres", "critères", "grille d evaluation", "grille d'évaluation", "notation", "bareme", "barème"},
    "candidature": {"candidature", "soumission", "dossier a soumettre", "dossier à soumettre", "deadline", "date limite", "contact"},
    "taches_table": {"taches_table", "tâches_table", "table_taches", "table:taches", "table:tâches"},
}

def _strip_accents_basic(s: str) -> str:
    # sans dépendance: mapping minimal utile
    return (
        s.replace("é", "e").replace("è", "e").replace("ê", "e")
         .replace("à", "a").replace("â", "a")
         .replace("î", "i").replace("ï", "i")
         .replace("ô", "o")
         .replace("ù", "u").replace("û", "u")
         .replace("ç", "c")
    )

def normalize_section(section: str) -> str:
    s = (section or "").strip().lower()
    if not s:
        return ""

    # normaliser espaces/ponctuation
    s = re.sub(r"[\s\-_/]+", " ", s).strip()
    s0 = _strip_accents_basic(s)

    # match direct canon
    if s0 in _SECTION_ALIASES:
        return s0

    # match via aliases
    for canon, aliases in _SECTION_ALIASES.items():
        if s in aliases or s0 in { _strip_accents_basic(a.lower()) for a in aliases }:
            return canon

    # cas “table:xxx”
    if s0.startswith("table:"):
        tail = s0.split("table:", 1)[1].strip()
        tail = normalize_section(tail)
        return f"table:{tail}" if tail else "table:"

    return s0  # fallback

def normalize_filters(filters: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    f = dict(filters or {})
    sec = f.get("section")
    if isinstance(sec, str) and sec.strip():
        f["section"] = normalize_section(sec)
    return f
