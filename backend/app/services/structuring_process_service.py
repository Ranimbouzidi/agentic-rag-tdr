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
    clean_and_dedup_tasks,
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
    # 1) sections
    sections = split_into_sections(normalized)

    # 2) tasks : mission -> livrables -> competences -> full text
    source_for_tasks = (
        sections.get("mission")
        or sections.get("livrables")
        or sections.get("competences")
        or normalized
    )
    taches = extract_tasks(source_for_tasks)
    taches = clean_and_dedup_tasks(taches)

    # 3) competences keywords
    competences = extract_skills_from_text(normalized)

    # 4) table-first (si docling markdown dispo)
    if extracted_markdown and extracted_markdown.strip():
        sections = enrich_sections_from_markdown_tables(
            sections=sections,
            markdown=extracted_markdown,
        )

    # 5) procurement fallback seulement si markers
    if _looks_like_procurement(normalized):
        sections = procurement_fallback(normalized, sections, taches)

    return {
        "doc_type": "tdr",
        "sections": sections,
        "taches": taches,
        "competences": competences,
        "ami_fields": None,
    }


def structure_document(
    doc_id: str,
    extracted_text: str,
    processed_prefix: str,
    extracted_markdown: Optional[str],
    processed_bucket: str,
) -> str:
    # 0) doc_type depuis DB (source de vérité)
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
        # AMI => route dédiée (pas procurement)
        result = route_structuring("ami", normalized)
    elif doc_type in ("tdr", "unknown", "other"):
        # TDR-like => on garde ton orchestrateur (table-first + procurement gating)
        result = _structure_tdr_like(
            doc_id=doc_id,
            normalized=normalized,
            extracted_markdown=extracted_markdown,
        )
    else:
        # fallback safe
        result = _structure_tdr_like(
            doc_id=doc_id,
            normalized=normalized,
            extracted_markdown=extracted_markdown,
        )

    metadata = extract_metadata(normalized)


    # 3) Payload stable
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
            "contexte": (result.get("sections") or {}).get("contexte", ""),
            "mission": (result.get("sections") or {}).get("mission", ""),
            "livrables": (result.get("sections") or {}).get("livrables", ""),
            "profil": (result.get("sections") or {}).get("profil", ""),
            "competences": result.get("competences") or [],
            "taches": result.get("taches") or [],
        },
        "ami_fields": result.get("ami_fields"),
    }

    # 4) Upload
    key = f"{processed_prefix}structured/tdr_structured.json"
    upload_text(processed_bucket, key, json.dumps(payload, ensure_ascii=False, indent=2))

    # 5) DB status
    with engine.begin() as conn:
        conn.execute(
            documents.update()
            .where(documents.c.id == doc_id)
            .values(status="structured")
        )

    return key
