# backend/app/services/structuring_process_service.py
import json
import sqlalchemy as sa
from typing import Optional, Dict, Any

from app.services.db_service import engine, documents
from app.services.minio_service import upload_text

from app.services.structuring_service import (
    split_into_sections,
    extract_tasks,
    extract_skills_from_text,
    normalize_text,
    enrich_sections_from_markdown_tables,
    procurement_fallback,
)

from app.services.structuring_router import route_structuring
from app.services.metadata_service import extract_metadata


PROCUREMENT_MARKERS = [
    "appel d'offre",
    "appel d’offres",
    "dao",
    "dossier d'appel d'offres",
    "dossier d’appel d’offres",
    "offre technique",
    "offre financière",
    "offre financiere",
    "soumission",
    "marché",
    "marche",
    "lot",
    "dossier administratif",
]


def _looks_like_procurement(text: str) -> bool:
    lower = (text or "").lower()
    return any(m in lower for m in PROCUREMENT_MARKERS)


def _structure_tdr_like(
    doc_id: str,
    normalized: str,
    extracted_markdown: Optional[str],
) -> Dict[str, Any]:
    # 1) Split sections
    sections = split_into_sections(normalized)

    # 2) TABLE-FIRST enrich (AVANT tâches)
    if extracted_markdown and extracted_markdown.strip():
        sections = enrich_sections_from_markdown_tables(
            sections=sections,
            markdown=extracted_markdown,
        )

    # 3) Extraction taches (liste) - source prioritaire: taches section > mission > livrables > texte complet
    source_for_tasks = (
        (sections.get("taches") or "").strip()
        or (sections.get("taches_table") or "").strip()
        or (sections.get("mission") or "").strip()
        or (sections.get("livrables") or "").strip()
        or (sections.get("competences") or "").strip()
        or normalized
    )

    # extract_tasks() inclut déjà nettoyage + dédup si tu as pris la dernière version
    taches_list = extract_tasks(source_for_tasks)

    # 4) competences keywords (liste)
    competences_list = extract_skills_from_text(normalized)

    # 5) procurement fallback seulement si markers
    if _looks_like_procurement(normalized):
        sections = procurement_fallback(normalized, sections, taches_list)

    return {
        "doc_type": "tdr",
        "sections": sections,
        "taches": taches_list,
        "competences": competences_list,
        "ami_fields": None,
    }


def structure_document(
    doc_id: str,
    extracted_text: str,
    processed_prefix: str,
    extracted_markdown: Optional[str],
    processed_bucket: str,
) -> str:
    # 0) doc_type depuis DB
    with engine.begin() as conn:
        row = conn.execute(
            sa.select(documents.c.id, documents.c.doc_type).where(documents.c.id == doc_id)
        ).mappings().first()
        if not row:
            raise ValueError("doc_id not found in documents table")
        doc_type = (row.get("doc_type") or "unknown").lower()

    # 1) Normalisation
    normalized = normalize_text(extracted_text)

    # 2) Routage
    if doc_type == "ami":
        result = route_structuring("ami", normalized)
    elif doc_type in ("tdr", "unknown", "other"):
        result = _structure_tdr_like(
            doc_id=doc_id,
            normalized=normalized,
            extracted_markdown=extracted_markdown,
        )
    else:
        result = _structure_tdr_like(
            doc_id=doc_id,
            normalized=normalized,
            extracted_markdown=extracted_markdown,
        )

    # 3) Metadata
    metadata = extract_metadata(normalized)

    sections = result.get("sections") or {}

    # 4) Payload stable (✅ inclut toutes les sections)
    payload = {
        "doc_id": doc_id,
        "doc_type": result.get("doc_type", doc_type),
        "metadata": {
            "langue": metadata.get("langue"),
            "domaine": metadata.get("domaine"),
            "bailleur": metadata.get("bailleur"),
            "pays": metadata.get("pays"),
            "region": metadata.get("region"),
            "dates": metadata.get("dates", {"publication": None, "deadline": None}),
        },
        "sections": {
            "contexte": sections.get("contexte", ""),
            "mission": sections.get("mission", ""),
            "taches": sections.get("taches", ""),
            "livrables": sections.get("livrables", ""),
            "planning": sections.get("planning", ""),
            "profil": sections.get("profil", ""),
            "competences": sections.get("competences", ""),
            "evaluation": sections.get("evaluation", ""),
            "candidature": sections.get("candidature", ""),
            "taches_table": sections.get("taches_table", ""),
        },
        # ✅ listes (extraction)
        "competences": result.get("competences") or [],
        "taches": result.get("taches") or [],
        "ami_fields": result.get("ami_fields"),
    }

        # 5) Upload
    out_doc_type = (payload.get("doc_type") or doc_type).lower()
    if out_doc_type == "ami":
        key = f"{processed_prefix}structured/ami_structured.json"
    else:
        key = f"{processed_prefix}structured/tdr_structured.json"

    upload_text(processed_bucket, key, json.dumps(payload, ensure_ascii=False, indent=2))

