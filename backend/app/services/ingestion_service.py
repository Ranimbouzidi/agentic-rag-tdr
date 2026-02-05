import uuid
from pathlib import Path
import sqlalchemy as sa

from app.services.minio_service import ensure_buckets, upload_file
from app.services.db_service import engine, documents


def ingest_file(file_path: Path) -> str:
    doc_id = str(uuid.uuid4())

    # lezem i make sure l buckets are there before upload w idha ma mawjoudeen n3mlouhom create automatiquement 7ata idha l api est deploy fi cloud w ma3andouch access lminio console w ykoun 7ab yst3ml blob walla s3 via gateway w ma7abch y3ml configuration manuelle l buckets w ykounou idempotents 3la khater idha l bucket mawjouda ma y3mlouch error w idha ma mawjouda y3mlou create automatiquement
    ensure_buckets()

    # hedha document loader function eli bch tetsana3 doc_id jdida w tuploadi lfile lminio w tinserti record jdid fil base m3a status "uploaded" w doc_id houwa li bch yest3mlou baad fi processing w indexing w bch ykoun unique identifier mta3 document mtei
    upload_file(
        bucket="tdr-raw",
        object_name=f"{doc_id}/{file_path.name}",
        file_path=str(file_path),
    )

    # insertion en base
    with engine.begin() as conn:
        conn.execute(
            documents.insert().values(
                id=doc_id,
                filename=file_path.name,
                status="uploaded",
            )
        )

    return doc_id
