import sqlalchemy as sa
from app.core.settings import settings

engine = sa.create_engine(settings.database_url, future=True)

metadata = sa.MetaData()

documents = sa.Table(
    "documents",
    metadata,
    sa.Column("id", sa.String, primary_key=True),

    # fichier & pipeline
    sa.Column("filename", sa.String, nullable=False),
    sa.Column("status", sa.String, nullable=False, index=True),

    # pointers storage (cloud-ready)
    sa.Column("raw_bucket", sa.String, nullable=False),
    sa.Column("raw_object_key", sa.String, nullable=False),
    sa.Column("processed_bucket", sa.String, nullable=False),
    sa.Column("processed_prefix", sa.String, nullable=False),

    # champs "curated" (remplis en phase 4)
    sa.Column("doc_type", sa.String, nullable=True),
    sa.Column("language", sa.String, nullable=True),
    sa.Column("title", sa.String, nullable=True),
    sa.Column("bailleur", sa.String, nullable=True),
    sa.Column("pays", sa.String, nullable=True),
    sa.Column("region", sa.String, nullable=True),
    sa.Column("domaine", sa.String, nullable=True),

    # audit
    sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now(), nullable=False),
    sa.Column("error_message", sa.Text, nullable=True),
)

def init_db():
    metadata.create_all(engine)
