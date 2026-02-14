from __future__ import annotations
import re
from typing import Dict, Any, List

from app.services.tracing import span_step
from app.services.structuring_service import extract_skills_from_text


def _extract_between(
    text: str,
    start_markers: List[str],
    end_markers: List[str],
    max_len: int = 3000,
) -> str:
    with span_step("structure.ami.extract_between", max_len=max_len):
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
    with span_step("structure.ami.extract_emails"):
        return sorted(set(re.findall(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", text or "")))


def extract_selection_method(text: str) -> str:
    with span_step("structure.ami.extract_selection_method"):
        t = (text or "").lower()
        if "qcbs" in t:
            return "QCBS"
        if "sfqc" in t:
            return "SFQC"
        return ""


def extract_deadline(text: str) -> str:
    with span_step("structure.ami.extract_deadline"):
        t = text or ""

        m = re.search(
            r"(avant le|au plus tard le)\s+(.{0,80}?)(?:\n|\.|;)",
            t,
            flags=re.IGNORECASE,
        )
        if m:
            return m.group(0).strip()

        m = re.search(
            r"\b(le)\s+\d{1,2}\s+[A-Za-zéèêàûîôç]+\s+\d{4}\s+(à|a)\s+\d{1,2}\s*h?\s*\d{0,2}",
            t,
            flags=re.IGNORECASE,
        )
        return m.group(0).strip() if m else ""


def extract_services_list(text: str, max_items: int = 30) -> list[str]:
    with span_step("structure.ami.extract_services_list", max_items=max_items):
        t = text or ""
        items = []
        seen = set()

        for m in re.finditer(r"(?m)^\s*(\d{1,2})\s*[.\-–]\s+(.+)$", t):
            s = m.group(2).strip()
            s = re.sub(r"\s+", " ", s)
            if 5 <= len(s) <= 220 and s.lower() not in seen:
                seen.add(s.lower())
                items.append(s)

        if len(items) < 3:
            for m in re.finditer(r"(?m)^\s*[▪•\-–]\s+(.+)$", t):
                s = m.group(1).strip()
                s = re.sub(r"\s+", " ", s)
                if 5 <= len(s) <= 220 and s.lower() not in seen:
                    seen.add(s.lower())
                    items.append(s)

        return items[:max_items]


def _extract_candidature_block(text: str, max_len: int = 2600) -> str:
    with span_step("structure.ami.extract_candidature_block", max_len=max_len):
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


def structure_ami(text: str) -> Dict[str, Any]:
    full = text or ""

    with span_step("structure.ami.structure_ami", in_len=len(full)) as span:
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

        taches_list = extract_services_list(mission_block or full)

        livrables = _extract_between(
            full,
            start_markers=["doivent fournir", "doivent fournir les informations", "brochures", "références", "references", "attestations", "certifications"],
            end_markers=["les critères", "criteres", "de plus amples informations", "adresse"],
            max_len=2000,
        )

        planning = _extract_between(
            full,
            start_markers=["date limite", "deadline", "au plus tard", "avant le"],
            end_markers=["adresse", "email", "e-mail", "de plus amples informations"],
            max_len=1800,
        )

        emails = extract_emails(full)
        selection_method = extract_selection_method(full)
        deadline = extract_deadline(full)
        skills = extract_skills_from_text(full)

        span.set_attribute("ami.tasks.count", len(taches_list))
        span.set_attribute("ami.emails.count", len(emails))
        span.set_attribute("ami.selection_method", selection_method)
        span.set_attribute("ami.deadline_present", bool(deadline))
        span.set_attribute("ami.skills.count", len(skills))

        return {
            "doc_type": "ami",
            "sections": {
                "contexte": contexte,
                "mission": mission_block,
                "taches": "",
                "livrables": livrables,
                "planning": planning,
                "profil": profil_block,
                "competences": "",
                "evaluation": criteres_block,
                "candidature": candidature_block,
                "taches_table": "",
            },
            "taches": taches_list,
            "competences": skills,
            "ami_fields": {
                "deadline": deadline,
                "selection_method": selection_method,
                "emails": emails,
                "criteres_selection": criteres_block,
            },
        }
