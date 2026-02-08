import json
import sqlalchemy as sa

from app.services.db_service import engine, documents
from app.services.minio_service import upload_text
from app.services.structuring_service import split_into_sections, extract_competences, extract_tasks, procurement_fallback
from app.services.structuring_service import extract_skills_from_text
from app.services.structuring_service import normalize_text, clean_and_dedup_tasks

 
def structure_document(doc_id: str, extracted_text: str, processed_prefix: str, processed_bucket: str) -> str:
    # 1) Normaliser le texte (puces, retours ligne, espaces)
    normalized = normalize_text(extracted_text)

    # 2) Split sections sur texte normalisé
    sections = split_into_sections(normalized)

    # 3) Extraction tâches : à partir section competences si présente, sinon tout le texte
    source_for_tasks = (
    sections.get("mission")
    or sections.get("livrables")
    or sections.get("competences")
    or normalized )
    taches = extract_tasks(source_for_tasks)
    taches = clean_and_dedup_tasks(taches)

    # 4) Compétences : extraction stable (keywords) sur le texte global
    competences = extract_skills_from_text(normalized)

    # 5) Fallback procurement sur texte normalisé + tâches nettoyées
    sections = procurement_fallback(normalized, sections, taches)

    payload = {
        "doc_id": doc_id,
        "metadata": {
            "langue": None,
            "domaine": None,
            "bailleur": None,
            "pays": None,
            "region": None,
            "dates": {"publication": None, "deadline": None},
        },
        "sections": {
            "contexte": sections.get("contexte", ""),
            "mission": sections.get("mission", ""),
            "livrables": sections.get("livrables", ""),
            "profil": sections.get("profil", ""),
            "competences": competences,
            "taches": taches,
        },
    }

    key = f"{processed_prefix}structured/tdr_structured.json"
    upload_text(processed_bucket, key, json.dumps(payload, ensure_ascii=False, indent=2))

    with engine.begin() as conn:
        conn.execute(
            documents.update()
            .where(documents.c.id == doc_id)
            .values(status="structured")
        )

    return key

