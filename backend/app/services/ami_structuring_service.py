from __future__ import annotations
import re
from typing import Dict, Any, Optional, List

from app.services.structuring_service import extract_skills_from_text


# -------------------------
# Helpers AMI
# -------------------------
def _extract_between(
    text: str,
    start_markers: List[str],
    end_markers: List[str],
    max_len: int = 3000,
) -> str:
    t = text or ""
    low = t.lower()

    starts = []
    for m in start_markers:
        p = low.find(m.lower())
        if p != -1:
            starts.append(p)
    if not starts:
        return ""

    start = min(starts)

    end_pos = None
    for em in end_markers:
        p = low.find(em.lower(), start + 10)
        if p != -1:
            end_pos = p if end_pos is None else min(end_pos, p)

    end = end_pos if end_pos is not None else min(len(t), start + max_len)
    return t[start:end].strip()


def extract_emails(text: str) -> list[str]:
    return sorted(
        set(re.findall(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", text or ""))
    )


def extract_selection_method(text: str) -> str:
    t = (text or "").lower()
    if "qcbs" in t:
        return "QCBS"
    if "sfqc" in t:
        return "SFQC"
    return ""


def extract_deadline(text: str) -> str:
    t = text or ""

    # 1) patterns "avant le ..." / "au plus tard le ..."
    m = re.search(
        r"(avant le|au plus tard le)\s+(.{0,80}?)(?:\n|\.|;)",
        t,
        flags=re.IGNORECASE,
    )
    if m:
        return m.group(0).strip()

    # 2) fallback: "le 07 février 2024 à 11h00"
    m = re.search(
        r"\b(le)\s+\d{1,2}\s+[A-Za-zéèêàûîôç]+\s+\d{4}\s+(à|a)\s+\d{1,2}\s*h?\s*\d{0,2}",
        t,
        flags=re.IGNORECASE,
    )
    return m.group(0).strip() if m else ""


def extract_services_list(text: str, max_items: int = 30) -> list[str]:
    """
    AMI: récupérer tâches à partir de listes numérotées + bullets.
    """
    t = text or ""
    items = []
    seen = set()

    # 1) items numérotés: "1. ..." ou "1- ..."
    for m in re.finditer(r"(?m)^\s*(\d{1,2})\s*[.\-–]\s+(.+)$", t):
        s = m.group(2).strip()
        s = re.sub(r"\s+", " ", s)
        if 5 <= len(s) <= 220 and s.lower() not in seen:
            seen.add(s.lower())
            items.append(s)

    # 2) bullets classiques si besoin
    if len(items) < 3:
        for m in re.finditer(r"(?m)^\s*[▪•\-–]\s+(.+)$", t):
            s = m.group(1).strip()
            s = re.sub(r"\s+", " ", s)
            if 5 <= len(s) <= 220 and s.lower() not in seen:
                seen.add(s.lower())
                items.append(s)

    return items[:max_items]


def _extract_candidature_block(text: str, max_len: int = 2600) -> str:
    """
    AMI: souvent il y a un passage sur comment soumettre / où envoyer / emails.
    """
    return _extract_between(
        text,
        start_markers=[
            "doivent être envoyées", "doivent etre envoyees", "envoyées à", "envoyees a",
            "adresse", "courrier électronique", "courrier electronique", "email", "e-mail",
            "manifestations d’intérêt doivent", "manifestations d'interet doivent",
        ],
        end_markers=[
            "de plus amples informations", "pour toute information", "signé", "fait à",
        ],
        max_len=max_len,
    )


# -------------------------
# Main AMI structurer
# -------------------------
def structure_ami(text: str) -> Dict[str, Any]:
    full = text or ""

    # blocs ancrés (évite chevauchements)
    contexte = _extract_between(
        full,
        start_markers=[
            "république", "programme", "prêt", "pret",
            "appel à manifestations", "manifestations d’intérêt", "manifestations d'interet",
        ],
        end_markers=[
            "les services comprennent", "les services incluent",
            "le ministère", "invite les firmes", "invite les firmes de consultants",
        ],
        max_len=2400,
    )

    mission_block = _extract_between(
        full,
        start_markers=["les services comprennent", "les services incluent", "les services comprennent :"],
        end_markers=["les termes de référence", "le ministère", "invite les firmes", "les critères", "criteres"],
        max_len=2400,
    )

    profil_block = _extract_between(
        full,
        start_markers=["invite les firmes", "invite les firmes de consultants", "les consultants intéressés", "les consultants interesses"],
        end_markers=["les critères", "criteres", "manifestations d'intérêt", "manifestations d’interêt", "doivent être envoyées", "doivent etre envoyees"],
        max_len=2600,
    )

    criteres_block = _extract_between(
        full,
        start_markers=["les critères", "criteres", "barème", "bareme", "poids", "tableau suivant"],
        end_markers=["de plus amples informations", "adresse", "doivent être envoyées", "doivent etre envoyees", "au plus tard", "avant le"],
        max_len=3000,
    )

    candidature_block = _extract_candidature_block(full, max_len=2400)

    # tasks/services (numéroté + bullets)
    taches_list = extract_services_list(mission_block or full)

    # livrables AMI = souvent pièces à fournir + références
    livrables = _extract_between(
        full,
        start_markers=["doivent fournir", "doivent fournir les informations", "brochures", "références", "references", "attestations", "certifications"],
        end_markers=["les critères", "criteres", "de plus amples informations", "adresse"],
        max_len=2000,
    )

    # (optionnel) planning : si on détecte des dates/délais, on met le texte autour
    planning = _extract_between(
        full,
        start_markers=["date limite", "deadline", "au plus tard", "avant le"],
        end_markers=["adresse", "email", "e-mail", "de plus amples informations"],
        max_len=1800,
    )

    return {
        "doc_type": "ami",
        "sections": {
            # ✅ mêmes clés que TDR pour stabilité
            "contexte": contexte,
            "mission": mission_block,
            "taches": "",          # section texte (souvent pas un vrai heading AMI)
            "livrables": livrables,
            "planning": planning,
            "profil": profil_block,
            "competences": "",     # texte (si jamais tu ajoutes plus tard)
            "evaluation": criteres_block,
            "candidature": candidature_block,
            "taches_table": "",    # pour compat, même si AMI n'a pas forcément de tables
        },
        "taches": taches_list,
        "competences": extract_skills_from_text(full),
        "ami_fields": {
            "deadline": extract_deadline(full),
            "selection_method": extract_selection_method(full),
            "emails": extract_emails(full),
            "criteres_selection": criteres_block,
        },
    }
