# backend/app/services/search_service.py
from __future__ import annotations

import json
import math
import re
from typing import Any, Dict, List, Optional, Tuple

import sqlalchemy as sa
from qdrant_client import QdrantClient
from qdrant_client.http import models as qm

from app.core.settings import settings
from app.services.db_service import engine, documents
from app.services.minio_service import download_text
from app.services.embedding_service import embed_batch
from app.services.bm25_service import bm25_scores
from app.services.tracing import span_step


# -------------------------
# Utils
# -------------------------
_word_rx = re.compile(r"[A-Za-zÀ-ÿ0-9']{2,}", re.UNICODE)


def _tokens(s: str) -> List[str]:
    return [t.lower() for t in _word_rx.findall(s or "")]


def _contains_all_filters(doc_row: Dict[str, Any], filters: Dict[str, Any]) -> bool:
    for k in ["doc_type", "pays", "bailleur", "domaine", "region", "language"]:
        v = filters.get(k)
        if not v:
            continue
        if (doc_row.get(k) or "").lower() != str(v).lower():
            return False
    return True


def _keyword_score(query_tokens: List[str], text: str) -> float:
    if not query_tokens:
        return 0.0

    lower = (text or "").lower()
    score = 0.0

    for tok in query_tokens:
        p = lower.find(tok)
        if p >= 0:
            score += 1.0 / math.log(3.0 + p)

    q = " ".join(query_tokens)
    if len(q) >= 10 and q in lower:
        score += 0.8

    if any("@" in t for t in query_tokens) and "@" in lower:
        score += 0.4
    if any(re.match(r"\d{4}", t) for t in query_tokens) and re.search(r"\b\d{4}\b", lower):
        score += 0.25

    return score


def _minmax_norm(values: List[float]) -> List[float]:
    if not values:
        return []
    vmin = min(values)
    vmax = max(values)
    if abs(vmax - vmin) < 1e-9:
        return [0.0 for _ in values]
    return [(v - vmin) / (vmax - vmin) for v in values]


def _dedup_by_doc_id(items: List[Dict[str, Any]], max_per_doc: int = 3) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    counts: Dict[str, int] = {}
    for it in items:
        doc_id = str(it.get("doc_id") or "")
        counts.setdefault(doc_id, 0)
        if counts[doc_id] >= max_per_doc:
            continue
        out.append(it)
        counts[doc_id] += 1
    return out


def _make_snippet(text: str, query: str, max_len: int = 320) -> str:
    t = (text or "").strip()
    if not t:
        return ""
    q = (query or "").strip().lower()
    low = t.lower()
    p = low.find(q) if len(q) >= 3 else -1
    if p < 0:
        return t[:max_len]
    start = max(0, p - 80)
    end = min(len(t), start + max_len)
    return ("..." if start > 0 else "") + t[start:end] + ("..." if end < len(t) else "")


def _group_results_by_doc(items: List[Dict[str, Any]], query: str, per_doc_snippets: int = 3) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}

    for it in items:
        doc_id = it.get("doc_id")
        if not doc_id:
            continue

        g = grouped.get(doc_id)
        if not g:
            grouped[doc_id] = {
                "doc_id": doc_id,
                "doc_type": it.get("doc_type"),
                "score": float(it.get("score_final") or it.get("score") or 0.0),
                "metadata": it.get("metadata") or {},
                "snippets": [],
            }
            g = grouped[doc_id]

        sc = float(it.get("score_final") or it.get("score") or 0.0)
        if sc > float(g.get("score") or 0.0):
            g["score"] = sc
            if it.get("doc_type"):
                g["doc_type"] = it.get("doc_type")
            if it.get("metadata"):
                g["metadata"] = it.get("metadata")

        g["snippets"].append(
            {
                "section": it.get("section"),
                "chunk_index": it.get("chunk_index"),
                "score_vector": it.get("score_vector"),
                "score_bm25": it.get("score_bm25"),
                "score": sc,
                "snippet": _make_snippet(it.get("text") or "", query=query),
            }
        )

    for g in grouped.values():
        g["snippets"].sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
        g["snippets"] = g["snippets"][:per_doc_snippets]

    docs = list(grouped.values())
    docs.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
    return docs


# -------------------------
# Qdrant helpers
# -------------------------
def _get_qdrant() -> QdrantClient:
    return QdrantClient(url=settings.qdrant_url)


def _build_qdrant_filter(filters: Dict[str, Any]) -> Optional[qm.Filter]:
    must: List[qm.Condition] = []
    should: List[qm.Condition] = []

    def add_kw(field: str, value: Any):
        if value is None or value == "":
            return
        must.append(qm.FieldCondition(key=field, match=qm.MatchValue(value=value)))

    add_kw("doc_type", filters.get("doc_type"))

    section = (filters.get("section") or "").strip()
    if section:
        should.append(qm.FieldCondition(key="section", match=qm.MatchValue(value=section)))
        should.append(qm.FieldCondition(key="section", match=qm.MatchValue(value=f"table:{section}")))
        if section == "taches":
            should.append(qm.FieldCondition(key="section", match=qm.MatchValue(value="tache:item")))

    add_kw("metadata.pays", filters.get("pays"))
    add_kw("metadata.bailleur", filters.get("bailleur"))
    add_kw("metadata.domaine", filters.get("domaine"))

    if not must and not should:
        return None

    if must and should:
        return qm.Filter(must=must, should=should)
    if must:
        return qm.Filter(must=must)
    return qm.Filter(should=should)


# -------------------------
# Search API
# -------------------------
def search(query: str, top_k: int = 8, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    filters = filters or {}
    query = (query or "").strip()
    if not query:
        raise ValueError("query is required")

    with span_step("search.request", top_k=top_k, query_len=len(query), filters_count=len(filters)) as span:
        w_vec = float(getattr(settings, "hybrid_w_vec", 0.70)) if hasattr(settings, "hybrid_w_vec") else 0.70
        w_bm25 = float(getattr(settings, "hybrid_w_lex", 0.30)) if hasattr(settings, "hybrid_w_lex") else 0.30
        pool_mult = int(getattr(settings, "hybrid_pool_mult", 8)) if hasattr(settings, "hybrid_pool_mult") else 8
        pool_k = max(top_k, top_k * pool_mult)

        per_doc_snippets = int(getattr(settings, "per_doc_snippets", 3)) if hasattr(settings, "per_doc_snippets") else 3
        max_per_doc_chunks = int(getattr(settings, "max_per_doc_chunks", 3)) if hasattr(settings, "max_per_doc_chunks") else 3

        span.set_attribute("hybrid.w_vec", w_vec)
        span.set_attribute("hybrid.w_bm25", w_bm25)
        span.set_attribute("hybrid.pool_k", pool_k)

        # 1) Qdrant hybrid
        try:
            with span_step("search.embed_query", model=getattr(settings, "embed_model", ""), batch_size=1):
                vec = embed_batch([query])[0]

            qf = _build_qdrant_filter(filters)

            with span_step("qdrant.query_points", collection=settings.qdrant_collection, limit=pool_k, has_filter=bool(qf)):
                qc = _get_qdrant()
                qr = qc.query_points(
                    collection_name=settings.qdrant_collection,
                    query=vec,
                    limit=pool_k,
                    with_payload=True,
                    query_filter=qf,
                )

            points = getattr(qr, "points", None) or []
            if not points:
                return {
                    "mode": "qdrant_hybrid_bm25",
                    "query": query,
                    "top_k": top_k,
                    "filters": filters,
                    "weights": {"vector": w_vec, "bm25": w_bm25},
                    "pool_k": pool_k,
                    "results": [],
                    "note": "Qdrant returned 0 points (filters too strict or collection empty).",
                }

            with span_step("search.build_candidates", points_count=len(points)):
                candidates: List[Dict[str, Any]] = []
                texts: List[str] = []
                vec_scores: List[float] = []

                for p in points:
                    payload = getattr(p, "payload", None) or {}
                    text = payload.get("text") or ""
                    tt = (text or "").strip()

                    # ignore junk chunks
                    if re.fullmatch(r"[\s\|\-:–—_]+", tt):
                        continue
                    if (
                        len(re.findall(r"[A-Za-zÀ-ÿ0-9]", tt)) < 12
                        and not re.search(r"[A-Za-zÀ-ÿ]{4,}", tt)
                    ):
                        continue

                    s_vec = float(getattr(p, "score", 0.0))

                    candidates.append(
                        {
                            "doc_id": payload.get("doc_id"),
                            "doc_type": payload.get("doc_type"),
                            "section": payload.get("section"),
                            "chunk_index": payload.get("chunk_index"),
                            "text": text,
                            "metadata": payload.get("metadata") or {},
                            "score_vector": s_vec,
                        }
                    )
                    texts.append(str(text))
                    vec_scores.append(s_vec)

            with span_step("search.bm25_scores", pool_texts=len(texts)):
                bm25_raw = bm25_scores(query, texts)

            with span_step("search.fuse_scores", candidates=len(candidates)):
                vnorm = _minmax_norm(vec_scores)
                bnorm = _minmax_norm(bm25_raw)

                for i, it in enumerate(candidates):
                    it["score_bm25"] = float(bm25_raw[i])
                    it["score"] = (w_vec * vnorm[i]) + (w_bm25 * bnorm[i])
                    it["score_final"] = it["score"]

                candidates.sort(key=lambda x: float(x.get("score_final") or 0.0), reverse=True)

            with span_step("search.dedup_by_doc", max_per_doc=max_per_doc_chunks):
                candidates = _dedup_by_doc_id(candidates, max_per_doc=max_per_doc_chunks)

            with span_step("search.group_results", per_doc_snippets=per_doc_snippets):
                grouped = _group_results_by_doc(candidates, query=query, per_doc_snippets=per_doc_snippets)

            return {
                "mode": "qdrant_hybrid_bm25",
                "query": query,
                "top_k": top_k,
                "filters": filters,
                "weights": {"vector": w_vec, "bm25": w_bm25},
                "pool_k": pool_k,
                "results": grouped[:top_k],
            }

        except Exception as qerr:
            span.set_attribute("search.qdrant_error", str(qerr))
            return _fallback_search(query=query, top_k=top_k, filters=filters, qdrant_error=str(qerr))


def _fallback_search(query: str, top_k: int, filters: Dict[str, Any], qdrant_error: str) -> Dict[str, Any]:
    MAX_DOCS = int(getattr(settings, "fallback_max_docs", 50)) if hasattr(settings, "fallback_max_docs") else 50

    with span_step("search.fallback", top_k=top_k, max_docs=MAX_DOCS) as span:
        q_tokens = _tokens(query)
        if not q_tokens:
            q_tokens = [query.lower()]

        # 1) select docs from DB
        with span_step("search.fallback.load_db_docs", limit=MAX_DOCS):
            with engine.begin() as conn:
                rows = conn.execute(
                    sa.select(
                        documents.c.id,
                        documents.c.doc_type,
                        documents.c.language,
                        documents.c.pays,
                        documents.c.bailleur,
                        documents.c.domaine,
                        documents.c.region,
                        documents.c.processed_bucket,
                        documents.c.processed_prefix,
                        documents.c.updated_at,
                    )
                    .order_by(documents.c.updated_at.desc())
                    .limit(MAX_DOCS)
                ).mappings().all()

        candidates: List[Dict[str, Any]] = []
        scanned = 0

        for r in rows:
            r = dict(r)
            if not _contains_all_filters(r, filters):
                continue

            scanned += 1
            bucket = r["processed_bucket"]
            prefix = r["processed_prefix"]
            structured_key = f"{prefix}structured/tdr_structured.json"

            try:
                with span_step("search.fallback.load_structured", bucket=bucket, key=structured_key):
                    raw = download_text(bucket, structured_key)
                    structured = json.loads(raw)
            except Exception:
                continue

            doc_type = (structured.get("doc_type") or r.get("doc_type") or "unknown")
            sections = structured.get("sections") or {}
            metadata = structured.get("metadata") or {}

            wanted_section = (filters.get("section") or "").strip()
            section_items: List[Tuple[str, Any]] = []

            if wanted_section:
                content = sections.get(wanted_section)
                if (content is None or content == "") and wanted_section == "taches":
                    content = structured.get("taches")
                section_items = [(wanted_section, content)]
            else:
                for sec in [
                    "mission",
                    "livrables",
                    "planning",
                    "profil",
                    "contexte",
                    "evaluation",
                    "candidature",
                    "taches_table",
                ]:
                    if sec in sections:
                        section_items.append((sec, sections.get(sec)))

                if "taches" in sections:
                    section_items.append(("taches", sections.get("taches")))
                elif structured.get("taches") is not None:
                    section_items.append(("taches", structured.get("taches")))

            for sec, content in section_items:
                if not content:
                    continue

                if isinstance(content, list):
                    content_text = "\n- " + "\n- ".join(str(x) for x in content[:40])
                else:
                    content_text = str(content)

                sc = _keyword_score(q_tokens, content_text)
                if sc <= 0:
                    continue

                candidates.append(
                    {
                        "score": float(sc),
                        "score_final": float(sc),
                        "doc_id": structured.get("doc_id") or r["id"],
                        "doc_type": doc_type,
                        "section": sec,
                        "chunk_index": None,
                        "text": content_text[:2000],
                        "metadata": metadata,
                    }
                )

        candidates.sort(key=lambda x: float(x.get("score_final") or 0.0), reverse=True)
        top_items = candidates[:top_k]

        per_doc_snippets = int(getattr(settings, "per_doc_snippets", 3)) if hasattr(settings, "per_doc_snippets") else 3
        grouped = _group_results_by_doc(top_items, query=query, per_doc_snippets=per_doc_snippets)

        span.set_attribute("fallback.docs_scanned", scanned)
        span.set_attribute("fallback.candidates", len(candidates))
        span.set_attribute("fallback.returned_docs", len(grouped))

        return {
            "mode": "fallback_lexical",
            "query": query,
            "top_k": top_k,
            "filters": filters,
            "qdrant_error": qdrant_error,
            "results": grouped,
            "note": f"Fallback limited to last {MAX_DOCS} docs for performance.",
        }
