# backend/app/services/bm25_service.py
from __future__ import annotations

import re
from typing import List
from rank_bm25 import BM25Okapi

_word_rx = re.compile(r"[A-Za-zÀ-ÿ0-9']{2,}", re.UNICODE)

def tokenize(text: str) -> List[str]:
    return [t.lower() for t in _word_rx.findall(text or "")]

def bm25_scores(query: str, docs: List[str]) -> List[float]:
    """
    BM25Okapi sur une liste de documents (petite liste, ex: pool 40).
    Retourne un score par doc (float).
    """
    if not docs:
        return []

    tokenized_corpus = [tokenize(d) for d in docs]
    bm25 = BM25Okapi(tokenized_corpus)

    q = tokenize(query)
    if not q:
        # requête vide/noisy -> scores 0
        return [0.0] * len(docs)

    scores = bm25.get_scores(q)
    return [float(s) for s in scores]
