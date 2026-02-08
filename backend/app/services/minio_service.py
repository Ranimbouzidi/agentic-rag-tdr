from minio import Minio
from app.core.settings import settings
from pathlib import Path
import io
from typing import Optional


# hedhy l service mta3 minio eli bch y3awenna nconnectiw lminio w n3mlou upload lfiles
# w nensureiw eli les buckets eli 7atithom fil settings mawjoudeen w idha ma mawjoudeen n3mlouhom create automatiquement
# cloud ready: compatible S3/minio gateways + idempotent bucket creation


def get_minio_client() -> Minio:
    endpoint = settings.minio_endpoint.replace("http://", "").replace("https://", "")
    return Minio(
        endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_endpoint.startswith("https://"),
    )


def ensure_buckets():
    client = get_minio_client()
    for bucket in [settings.minio_bucket_raw, settings.minio_bucket_processed]:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)


def upload_file(bucket: str, object_name: str, file_path: str):
    client = get_minio_client()
    client.fput_object(bucket, object_name, file_path)


def download_file(bucket: str, object_name: str, dest_path: Path):
    client = get_minio_client()
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    client.fget_object(bucket, object_name, str(dest_path))


# -------------------------
# TEXT HELPERS (Phase 4A)
# -------------------------
def upload_text(
    bucket: str,
    object_name: str,
    text: str,
    content_type: str = "text/plain; charset=utf-8",
):
    """
    Upload du texte en MinIO.
    content_type configurable (text/plain, text/markdown).
    """
    client = get_minio_client()
    data = (text or "").encode("utf-8")
    client.put_object(
        bucket,
        object_name,
        io.BytesIO(data),
        length=len(data),
        content_type=content_type,
    )


def upload_markdown(bucket: str, object_name: str, markdown: str):
    """
    Helper dédié Markdown (Docling -> extracted.md)
    """
    upload_text(bucket, object_name, markdown, content_type="text/markdown; charset=utf-8")


def download_text(
    bucket: str,
    object_name: str,
    encoding: str = "utf-8",
    errors: str = "replace",
) -> str:
    """
    Télécharge un objet texte depuis MinIO et le retourne en string.
    Ferme/release correctement la connexion.
    """
    client = get_minio_client()
    resp = None
    try:
        resp = client.get_object(bucket, object_name)
        return resp.read().decode(encoding, errors=errors)
    finally:
        if resp is not None:
            resp.close()
            resp.release_conn()


def object_exists(bucket: str, object_name: str) -> bool:
    """
    True si l’objet existe dans le bucket.
    """
    client = get_minio_client()
    try:
        client.stat_object(bucket, object_name)
        return True
    except Exception:
        return False
