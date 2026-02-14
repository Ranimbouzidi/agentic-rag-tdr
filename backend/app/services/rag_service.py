# backend/app/services/rag_service.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import uuid
import httpx
import re

from qdrant_client import QdrantClient

from app.core.settings import settings
from app.services.search_service import search
from app.services.tracing import span_step


# -------------------------
# Qdrant helpers
# -------------------------
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


def _fetch_chunks_by_ids(
    snippet_refs: List[Tuple[str, str, int]]
) -> Dict[Tuple[str, str, int], Dict[str, Any]]:
    """
    Retrieve ONLY the needed chunks from Qdrant using point_ids.
    key = (doc_id, section, chunk_index) -> payload
    """
    if not snippet_refs:
        return {}

    with span_step("rag.fetch_chunks_by_ids", refs_count=len(snippet_refs)):
        qc = _get_qdrant()

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

        with span_step("qdrant.retrieve", collection=settings.qdrant_collection, ids_count=len(ids)):
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


def _neighbor_refs(doc_id: str, section: str, center_idx: int, radius: int = 2) -> List[Tuple[str, str, int]]:
    refs: List[Tuple[str, str, int]] = []
    for d in range(1, radius + 1):
        refs.append((doc_id, section, center_idx - d))
        refs.append((doc_id, section, center_idx + d))
    return [(a, b, c) for (a, b, c) in refs if isinstance(c, int) and c >= 0]


# -------------------------
# Context builder (generic)
# -------------------------
def _build_context_from_grouped_results(
    grouped_docs: List[Dict[str, Any]],
    query: str,
) -> Tuple[str, List[Dict[str, Any]]]:
    with span_step("rag.build_context", query_len=len(query or ""), docs_count=len(grouped_docs or [])) as span:
        max_docs = int(getattr(settings, "rag_top_docs", 1))
        per_doc = int(getattr(settings, "rag_snippets_per_doc", 2))
        max_chars = int(getattr(settings, "rag_max_context_chars", 1500))
        max_chunk_chars = int(getattr(settings, "rag_max_chunk_chars", 2500))
        expand_radius = int(getattr(settings, "rag_expand_radius", 1))

        selected_docs = (grouped_docs or [])[:max_docs]

        wanted: List[Tuple[str, str, int]] = []  # qdrant keys to retrieve
        base_items: List[Dict[str, Any]] = []    # hits coming from search (with scores)

        # 1) Collect top snippets from search()
        with span_step("rag.collect_top_snippets", selected_docs=len(selected_docs), per_doc=per_doc):
            for d in selected_docs:
                doc_id = d.get("doc_id")
                if not doc_id:
                    continue

                doc_type = d.get("doc_type") or "unknown"
                doc_meta = d.get("metadata") or {}
                snippets_all = list(d.get("snippets") or [])

                snippets_all.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
                top_snips = snippets_all[:per_doc]

                for s in top_snips:
                    section = s.get("section")
                    chunk_index = s.get("chunk_index")
                    if not section:
                        continue

                    score = s.get("score")  # keep search score

                    if isinstance(chunk_index, int):
                        wanted.append((doc_id, section, chunk_index))
                        base_items.append(
                            {
                                "doc_id": doc_id,
                                "doc_type": doc_type,
                                "metadata": doc_meta,
                                "section": section,
                                "chunk_index": chunk_index,
                                "score": score,
                                "snippet": s.get("snippet"),
                            }
                        )
                    else:
                        # fallback lexical: no chunk_index => only snippet text available
                        base_items.append(
                            {
                                "doc_id": doc_id,
                                "doc_type": doc_type,
                                "metadata": doc_meta,
                                "section": section,
                                "chunk_index": None,
                                "score": score,
                                "snippet": s.get("snippet"),
                            }
                        )

        # 2) Expand around best hit (neighbors)
        with span_step("rag.expand_neighbors", expand_radius=expand_radius):
            best = None
            best_score = -1.0

            for it in base_items:
                if it.get("chunk_index") is None:
                    continue
                sc = float(it.get("score") or 0.0)
                if sc > best_score:
                    best = it
                    best_score = sc

            if best:
                sec = str(best.get("section") or "")
                if not sec.startswith("table:") and sec != "tache:item":
                    wanted.extend(
                        _neighbor_refs(
                            best["doc_id"],
                            sec,
                            int(best["chunk_index"]),
                            radius=expand_radius,
                        )
                    )

        # de-dup wanted
        wanted = list(dict.fromkeys(wanted))
        span.set_attribute("rag.wanted_refs_count", len(wanted))

        # 3) retrieve only needed qdrant chunks
        chunks_payload = _fetch_chunks_by_ids(wanted)

        # 4) Build ordered list
        with span_step("rag.order_chunks"):
            base_keys: List[Tuple[str, str, int]] = []
            base_score_by_key: Dict[Tuple[str, str, int], Optional[float]] = {}

            for it in base_items:
                if isinstance(it.get("chunk_index"), int):
                    k = (it["doc_id"], it["section"], int(it["chunk_index"]))
                    base_keys.append(k)
                    base_score_by_key[k] = it.get("score")

            seen = set()
            base_keys_unique: List[Tuple[str, str, int]] = []
            for k in base_keys:
                if k not in seen:
                    base_keys_unique.append(k)
                    seen.add(k)

            ordered_qdrant_keys: List[Tuple[str, str, int]] = []
            seen2 = set()
            for k in base_keys_unique:
                if k not in seen2:
                    ordered_qdrant_keys.append(k)
                    seen2.add(k)
            for k in wanted:
                if k not in seen2:
                    ordered_qdrant_keys.append(k)
                    seen2.add(k)

        # 5) Emit context + sources
        with span_step("rag.emit_context", max_chars=max_chars, max_chunk_chars=max_chunk_chars):
            context_parts: List[str] = []
            sources: List[Dict[str, Any]] = []
            total = 0

            for (doc_id, section, chunk_index) in ordered_qdrant_keys:
                payload = chunks_payload.get((doc_id, section, chunk_index)) or {}
                full_text = (payload.get("text") or "").strip()

                # junk guards
                if full_text and re.fullmatch(r"[\s\|\-:–—_]+", full_text):
                    continue
                if full_text and len(re.findall(r"[A-Za-zÀ-ÿ0-9]", full_text)) < 12 and not re.search(r"[A-Za-zÀ-ÿ]{4,}", full_text):
                    continue

                if not full_text:
                    # fallback to snippet if base hit
                    sn = next(
                        (
                            x for x in base_items
                            if x.get("doc_id") == doc_id
                            and x.get("section") == section
                            and x.get("chunk_index") == chunk_index
                        ),
                        None,
                    )
                    full_text = (sn.get("snippet") if sn else "") or ""
                    full_text = full_text.strip()

                if not full_text:
                    continue

                if max_chunk_chars and len(full_text) > max_chunk_chars:
                    full_text = full_text[:max_chunk_chars]

                block = (
                    f"{full_text}\n"
                    f"[SOURCE doc_id={doc_id} section={section} chunk_index={chunk_index}]\n"
                )

                if total + len(block) > max_chars:
                    break

                context_parts.append(block)
                total += len(block)

                src_score = next(
                    (
                        float(x.get("score"))
                        for x in base_items
                        if x["doc_id"] == doc_id
                        and x["section"] == section
                        and x.get("chunk_index") == chunk_index
                        and x.get("score") is not None
                    ),
                    None,
                )

                sources.append(
                    {
                        "doc_id": doc_id,
                        "doc_type": payload.get("doc_type"),
                        "section": section,
                        "chunk_index": chunk_index,
                        "score": src_score,  # neighbors => None
                        "metadata": payload.get("metadata") or {},
                        "snippet": (payload.get("text") or "")[:400],
                    }
                )

            # 6) Add fallback lexical-only sources
            for it in base_items:
                if it.get("chunk_index") is not None:
                    continue

                full_text = (it.get("snippet") or "").strip()
                if not full_text:
                    continue

                if max_chunk_chars and len(full_text) > max_chunk_chars:
                    full_text = full_text[:max_chunk_chars]

                block = (
                    f"{full_text}\n"
                    f"[SOURCE doc_id={it['doc_id']} section={it['section']} chunk_index=None]\n"
                )

                if total + len(block) > max_chars:
                    break

                context_parts.append(block)
                total += len(block)

                sources.append(
                    {
                        "doc_id": it["doc_id"],
                        "doc_type": it.get("doc_type"),
                        "section": it.get("section"),
                        "chunk_index": None,
                        "score": it.get("score"),
                        "metadata": it.get("metadata") or {},
                        "snippet": full_text[:400],
                    }
                )

            span.set_attribute("rag.context_chars", total)
            span.set_attribute("rag.sources_count", len(sources))

            return "\n---\n".join(context_parts), sources


# -------------------------
# Ollama call
# -------------------------
def _ollama_generate(prompt: str) -> str:
    url = settings.ollama_base_url.rstrip("/") + "/api/generate"

    timeout_s = float(getattr(settings, "llm_timeout_s", 1800))
    num_predict = int(getattr(settings, "llm_num_predict", 384))
    temperature = float(getattr(settings, "rag_temperature", 0.2))

    payload = {
        "model": settings.llm_model,
        "prompt": prompt,
        "stream": False,
        "keep_alive": "10m",
        "options": {
            "temperature": temperature,
            "num_predict": num_predict,
        },
    }

    timeout = httpx.Timeout(
        timeout_s,
        connect=30.0,
        read=timeout_s,
        write=30.0,
        pool=timeout_s,
    )

    with span_step(
        "llm.ollama.generate",
        model=settings.llm_model,
        prompt_len=len(prompt or ""),
        temperature=temperature,
        num_predict=num_predict,
    ) as span:
        try:
            with httpx.Client(timeout=timeout) as client:
                response = client.post(url, json=payload)
                span.set_attribute("http.status_code", response.status_code)
                response.raise_for_status()
                data = response.json()
                out = (data.get("response") or "").strip()
                span.set_attribute("llm.answer_len", len(out))
                return out

        except httpx.ReadTimeout:
            span.set_attribute("error.type", "ReadTimeout")
            return "Je ne sais pas."

        except httpx.HTTPStatusError as e:
            span.set_attribute("error.type", "HTTPStatusError")
            span.set_attribute("error.status_code", int(e.response.status_code))
            return "Je ne sais pas."

        except httpx.RequestError as e:
            span.set_attribute("error.type", "RequestError")
            span.set_attribute("error.message", str(e))
            return "Je ne sais pas."

        except Exception as e:
            span.set_attribute("error.type", type(e).__name__)
            span.set_attribute("error.message", str(e))
            return "Je ne sais pas."


# -------------------------
# Public API
# -------------------------
def answer(query: str, top_k: int = 5, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    filters = filters or {}
    query = (query or "").strip()

    with span_step("rag.answer", top_k=top_k) as span:
        span.set_attribute("query.len", len(query))
        span.set_attribute("filters.count", len(filters))

        with span_step("rag.search", top_k=top_k):
            sr = search(query=query, top_k=top_k, filters=filters)

        grouped_docs = sr.get("results") or []

        context, sources = _build_context_from_grouped_results(grouped_docs, query=query)

        prompt = (
            "Tu es un assistant.\n"
            "Réponds uniquement avec les informations présentes dans le CONTEXTE.\n"
            "Si la réponse n'est pas dans le CONTEXTE, réponds exactement: Je ne sais pas.\n"
            "Réponse attendue: une réponse concise.\n\n"
            f"QUESTION:\n{query}\n\n"
            f"CONTEXTE:\n{context}\n\n"
            "RÉPONSE:"
        )

        span.set_attribute("context.chars", len(context))
        span.set_attribute("sources.count", len(sources))

        with span_step("rag.llm_generate"):
            llm_answer = _ollama_generate(prompt)

        return {
            "query": query,
            "filters": filters,
            "mode": "rag_option_b_qdrant_retrieve_chunks",
            "top_k": top_k,
            "search_mode": sr.get("mode"),
            "answer": llm_answer,
            "sources": sources,
            "context_chars": len(context),
        }
