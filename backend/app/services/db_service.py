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

    # ✅ doc_type (métier) : tdr | ami | other | unknown
    sa.Column("doc_type", sa.String(16), nullable=False, server_default="unknown"),

    # pointers storage
    sa.Column("raw_bucket", sa.String, nullable=False),
    sa.Column("raw_object_key", sa.String, nullable=False),
    sa.Column("processed_bucket", sa.String, nullable=False),
    sa.Column("processed_prefix", sa.String, nullable=False),

    # champs "curated"
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


    # Phase 5 - indexing metadata
    sa.Column("qdrant_collection", sa.String(128), nullable=True),
    sa.Column("vector_size", sa.Integer, nullable=True),
    sa.Column("chunk_count", sa.Integer, nullable=True),
    sa.Column("indexed_at", sa.DateTime(timezone=True), nullable=True),

)

embedding_cache = sa.Table(
    "embedding_cache",
    metadata,
    sa.Column("id", sa.BigInteger, primary_key=True),
    sa.Column("model", sa.String(64), nullable=False),
    sa.Column("text_hash", sa.String(40), nullable=False),
    sa.Column("dim", sa.Integer, nullable=False),
    sa.Column("embedding", sa.JSON, nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
)



def init_db():
    metadata.create_all(engine)
