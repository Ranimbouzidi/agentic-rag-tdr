# backend/app/services/structuring_process_service.py
import json
import sqlalchemy as sa
from typing import Optional, Dict, Any

from app.services.tracing import span_step
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
    with span_step("structure.tdr_like", doc_id=doc_id, norm_len=len(normalized or ""), md_len=len(extracted_markdown or "")):
        # 1) Split sections
        with span_step("structure.split_sections", doc_id=doc_id):
            sections = split_into_sections(normalized)

        # 2) TABLE-FIRST enrich (AVANT tâches)
        if extracted_markdown and extracted_markdown.strip():
            with span_step("structure.enrich_tables", doc_id=doc_id):
                sections = enrich_sections_from_markdown_tables(
                    sections=sections,
                    markdown=extracted_markdown,
                )

        # 3) Extraction taches (liste) - source prioritaire
        source_for_tasks = (
            (sections.get("taches") or "").strip()
            or (sections.get("taches_table") or "").strip()
            or (sections.get("mission") or "").strip()
            or (sections.get("livrables") or "").strip()
            or (sections.get("competences") or "").strip()
            or normalized
        )

        with span_step("structure.extract_tasks_list", doc_id=doc_id, source_len=len(source_for_tasks or "")):
            taches_list = extract_tasks(source_for_tasks)

        # 4) competences keywords (liste)
        with span_step("structure.extract_competences_list", doc_id=doc_id, norm_len=len(normalized or "")):
            competences_list = extract_skills_from_text(normalized)

        # 5) procurement fallback seulement si markers
        if _looks_like_procurement(normalized):
            with span_step("structure.procurement_fallback_apply", doc_id=doc_id):
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
    with span_step("structure.load_doc_type", doc_id=doc_id):
        with engine.begin() as conn:
            row = conn.execute(
                sa.select(documents.c.id, documents.c.doc_type).where(documents.c.id == doc_id)
            ).mappings().first()
            if not row:
                raise ValueError("doc_id not found in documents table")
            doc_type = (row.get("doc_type") or "unknown").lower()

    # 1) Normalisation
    with span_step("structure.normalize", doc_id=doc_id, in_len=len(extracted_text or "")) as span:
        normalized = normalize_text(extracted_text)
        span.set_attribute("out_len", len(normalized or ""))

    # 2) Routage
    with span_step("structure.route", doc_id=doc_id, doc_type=doc_type) as span:
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
        span.set_attribute("result.doc_type", (result.get("doc_type") or doc_type))

    # 3) Metadata
    with span_step("structure.metadata", doc_id=doc_id, norm_len=len(normalized or "")) as span:
        metadata = extract_metadata(normalized)
        # attributs utiles mais légers
        span.set_attribute("meta.langue", metadata.get("langue"))
        span.set_attribute("meta.domaine", metadata.get("domaine"))
        span.set_attribute("meta.pays", metadata.get("pays"))
        span.set_attribute("meta.region", metadata.get("region"))
        span.set_attribute("meta.bailleur", metadata.get("bailleur"))

    sections = result.get("sections") or {}

    # 4) Payload stable (✅ inclut toutes les sections)
    with span_step("structure.build_payload", doc_id=doc_id):
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

    with span_step(
        "structure.upload_structured",
        doc_id=doc_id,
        bucket=processed_bucket,
        object_key=key,
        doc_type=out_doc_type,
        json_len=len(json.dumps(payload, ensure_ascii=False)),
    ):
        upload_text(processed_bucket, key, json.dumps(payload, ensure_ascii=False, indent=2))

    return key
