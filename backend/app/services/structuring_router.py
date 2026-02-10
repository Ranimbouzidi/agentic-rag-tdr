from __future__ import annotations

from typing import Dict, Any

from app.services.structuring_service import (
    split_into_sections,
    extract_tasks,
    clean_and_dedup_tasks,
    extract_skills_from_text,
    procurement_fallback,
)

from app.services.ami_structuring_service import structure_ami


def route_structuring(doc_type: str, normalized_text: str) -> Dict[str, Any]:
    dt = (doc_type or "unknown").lower()

    if dt == "ami":
        return structure_ami(normalized_text)

    # default: TDR (logique existante)
    sections = split_into_sections(normalized_text)

    source_for_tasks = sections.get("competences") or normalized_text
    taches = extract_tasks(source_for_tasks)
    taches = clean_and_dedup_tasks(taches)

    competences = extract_skills_from_text(normalized_text)

    # IMPORTANT: signature = (text, sections, tasks)
    sections = procurement_fallback(normalized_text, sections, taches)

    return {
        "doc_type": "tdr",
        "sections": sections,
        "taches": taches,
        "competences": competences,
        "ami_fields": None,
    }
