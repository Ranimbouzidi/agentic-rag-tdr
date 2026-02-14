from minio import Minio
from app.core.settings import settings
from pathlib import Path
import io

from app.services.tracing import span_step


def get_minio_client() -> Minio:
    endpoint = settings.minio_endpoint.replace("http://", "").replace("https://", "")
    return Minio(
        endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_endpoint.startswith("https://"),
    )


def ensure_buckets():
    with span_step("minio.ensure_buckets"):
        client = get_minio_client()
        for bucket in [settings.minio_bucket_raw, settings.minio_bucket_processed]:
            with span_step("minio.ensure_bucket", bucket=bucket):
                if not client.bucket_exists(bucket):
                    client.make_bucket(bucket)


def upload_file(bucket: str, object_name: str, file_path: str):
    with span_step(
        "minio.upload_file",
        bucket=bucket,
        object_key=object_name,
        file_path=str(file_path),
    ):
        client = get_minio_client()
        client.fput_object(bucket, object_name, file_path)


def download_file(bucket: str, object_name: str, dest_path: Path):
    with span_step(
        "minio.download_file",
        bucket=bucket,
        object_key=object_name,
        dest_path=str(dest_path),
    ):
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
    with span_step(
        "minio.upload_text",
        bucket=bucket,
        object_key=object_name,
        content_type=content_type,
        text_len=len(text or ""),
    ):
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
    with span_step(
        "minio.upload_markdown",
        bucket=bucket,
        object_key=object_name,
        md_len=len(markdown or ""),
    ):
        upload_text(bucket, object_name, markdown, content_type="text/markdown; charset=utf-8")


def download_text(bucket: str, object_name: str) -> str:
    with span_step("minio.download_text", bucket=bucket, object_key=object_name):
        client = get_minio_client()
        resp = None
        try:
            resp = client.get_object(bucket, object_name)
            raw = resp.read()

            # 1) try utf-8 strict
            try:
                return raw.decode("utf-8")
            except UnicodeDecodeError:
                pass

            # 2) try windows-1252 (common for FR docs)
            try:
                return raw.decode("cp1252")
            except UnicodeDecodeError:
                pass

            # 3) last resort
            return raw.decode("latin-1", errors="replace")

        finally:
            if resp is not None:
                resp.close()
                resp.release_conn()


def object_exists(bucket: str, object_name: str) -> bool:
    """
    True si l’objet existe dans le bucket.
    """
    with span_step("minio.object_exists", bucket=bucket, object_key=object_name):
        client = get_minio_client()
        try:
            client.stat_object(bucket, object_name)
            return True
        except Exception:
            return False
