from minio import Minio
from app.core.settings import settings
#hedhy l service mta3 minio eli bch y3awenna nconnectiw lminio w n3mlou upload lfiles w nensureiw eli les buckets eli 7atithom fil settings mawjoudeen w idha ma mawjoudeen n3mlouhom create automatiquement
#c est cloud ready ala khater l api est s compatible nafs l code ykhademha fi blob walla s3 via gateway wel lbuckets cree automatiquement nkoulou aalihom idempotents 

def get_minio_client() -> Minio:
    return Minio(
        settings.minio_endpoint.replace("http://", ""),
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=False,
    )


def ensure_buckets():
    client = get_minio_client()
    for bucket in [settings.minio_bucket_raw, settings.minio_bucket_processed]:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)


def upload_file(bucket: str, object_name: str, file_path: str):
    client = get_minio_client()
    client.fput_object(bucket, object_name, file_path)
