from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import uuid
import httpx

from qdrant_client import QdrantClient
from qdrant_client.http import models as qm

from app.core.settings import settings
from app.services.search_service import search


def _get_qdrant() -> QdrantClient:
    return QdrantClient(url=settings.qdrant_url)


def _point_id(doc_id: str, section: str, chunk_index: int) -> str:
    """
    Must match indexing_service.py:
      ns = UUID(doc_id)
      point_id = uuid5(ns, f"{doc_id}:{section}:{chunk_index}")
    """
    ns = uuid.UUID(doc_id)
    return str(uuid.uuid5(ns, f"{doc_id}:{section}:{chunk_index}"))


def _fetch_chunks_by_ids(snippet_refs: List[Tuple[str, str, int]]) -> Dict[Tuple[str, str, int], Dict[str, Any]]:
    """
    Retrieve ONLY the needed chunks from Qdrant using point_ids (fast).
    key = (doc_id, section, chunk_index) -> payload
    """
    qc = _get_qdrant()

    # build retrieve list
    ids: List[str] = []
    key_by_id: Dict[str, Tuple[str, str, int]] = {}

    for doc_id, section, chunk_index in snippet_refs:
        if not doc_id or not section or not isinstance(chunk_index, int):
            continue
        pid = _point_id(doc_id, section, chunk_index)
        ids.append(pid)
        key_by_id[pid] = (doc_id, section, chunk_index)

    if not ids:
        return {}

    points = qc.retrieve(
        collection_name=settings.qdrant_collection,
        ids=ids,
        with_payload=True,
        with_vectors=False,
    )

    out: Dict[Tuple[str, str, int], Dict[str, Any]] = {}
    for p in points:
        pid = str(getattr(p, "id", ""))
        key = key_by_id.get(pid)
        if not key:
            continue
        payload = getattr(p, "payload", None) or {}
        out[key] = payload

    return out


def _build_context_from_grouped_results(
    grouped_docs: List[Dict[str, Any]],
    query: str
) -> Tuple[str, List[Dict[str, Any]]]:

    max_docs = int(getattr(settings, "rag_top_docs", 1))
    per_doc = int(getattr(settings, "rag_snippets_per_doc", 3))
    max_chars = int(getattr(settings, "rag_max_context_chars", 4000))
    max_chunk_chars = int(getattr(settings, "rag_max_chunk_chars", 2500))

    selected_docs = (grouped_docs or [])[:max_docs]

    # Collect snippet refs (prioritize mission first)
    wanted: List[Tuple[str, str, int]] = []

    for d in selected_docs:
        doc_id = d.get("doc_id")
        if not doc_id:
            continue

        snippets_all = list(d.get("snippets") or [])

        # ✅ priorité mission
        snippets_all.sort(
            key=lambda x: 0 if x.get("section") in ("mission", "services", "taches") else 1
        )

        snippets = snippets_all[:per_doc]

        for s in snippets:
            section = s.get("section")
            chunk_index = s.get("chunk_index")

            if section and isinstance(chunk_index, int):
                wanted.append((doc_id, section, chunk_index))

    chunks_payload = _fetch_chunks_by_ids(wanted)

    context_parts: List[str] = []
    sources: List[Dict[str, Any]] = []
    total = 0

    for rank, d in enumerate(selected_docs, start=1):
        doc_id = d.get("doc_id")
        if not doc_id:
            continue

        doc_meta = d.get("metadata") or {}
        doc_type = d.get("doc_type")

        snippets_all = list(d.get("snippets") or [])
        snippets_all.sort(
            key=lambda x: 0 if x.get("section") in ("mission", "services", "taches") else 1
        )

        snippets = snippets_all[:per_doc]

        for s in snippets:
            section = s.get("section")
            chunk_index = s.get("chunk_index")

            full_text = ""

            if section and isinstance(chunk_index, int):
                payload = chunks_payload.get((doc_id, section, chunk_index)) or {}
                full_text = (payload.get("text") or "").strip()

            # fallback
            if not full_text:
                full_text = (s.get("snippet") or "").strip()

            # ✅ IMPORTANT: ne pas tronquer mission/services/taches
            if section not in ("mission", "services", "taches"):
                if max_chunk_chars and len(full_text) > max_chunk_chars:
                    full_text = full_text[:max_chunk_chars]

            block = (
                f"{full_text}\n"
                f"[SOURCE doc={rank} doc_id={doc_id} section={section} chunk_index={chunk_index}]\n"
            )

            if total + len(block) > max_chars:
                break

            context_parts.append(block)
            total += len(block)

            sources.append(
                {
                    "doc_id": doc_id,
                    "doc_type": doc_type,
                    "section": section,
                    "chunk_index": chunk_index,
                    "score": s.get("score"),
                    "metadata": doc_meta,
                    "snippet": s.get("snippet"),
                }
            )

        if total >= max_chars:
            break

    return "\n---\n".join(context_parts), sources



def _ollama_generate(prompt: str) -> str:
    url = settings.ollama_base_url.rstrip("/") + "/api/generate"

    timeout_s = float(getattr(settings, "llm_timeout_s", 350))
    num_predict = int(getattr(settings, "llm_num_predict", 1024))
    temperature = float(getattr(settings, "rag_temperature", 0.2))

    payload = {
        "model": settings.llm_model,
        "prompt": prompt,
        "stream": False,
        # ✅ empêche Ollama de décharger le modèle trop vite (évite “warmup” lent)
        "keep_alive": "10m",
        "options": {
            "temperature": temperature,
            "num_predict": num_predict,
        },
    }

    # ✅ timeout explicite (connect/read/write/pool)
    t = httpx.Timeout(timeout_s, connect=30.0, read=timeout_s, write=30.0, pool=timeout_s)
    with httpx.Client(timeout=t) as client:
        r = client.post(url, json=payload)
        r.raise_for_status()
        data = r.json()
        return (data.get("response") or "").strip()




def answer(query: str, top_k: int = 5, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    filters = filters or {}

    sr = search(query=query, top_k=top_k, filters=filters)
    grouped_docs = sr.get("results") or []

    context, sources = _build_context_from_grouped_results(grouped_docs, query=query)

    prompt = (
    "Tu es un assistant.\n"
    "Réponds uniquement avec les informations présentes dans le CONTEXTE.\n"
    "Si la réponse n'est pas dans le CONTEXTE, réponds exactement: Je ne sais pas.\n"
    "Réponse attendue: une liste complète et concise.\n\n"
    f"QUESTION:\n{query}\n\n"
    f"CONTEXTE:\n{context}\n\n"
    "RÉPONSE:"
)
    
    print("CTX_CHARS", len(context))
    print("CTX_PREVIEW\n", context[:800])
    print("CTX_END\n", context[-800:])
    llm_answer = _ollama_generate(prompt)

    return {
        "query": query,
        "filters": filters,
        "mode": "rag_option_b_qdrant_retrieve_chunks",
        "top_k": top_k,
        "search_mode": sr.get("mode"),
        "answer": llm_answer,
        "sources": sources,
        # debug (facultatif, tu peux enlever si tu veux)
        "context_chars": len(context),
    }
