import sqlalchemy as sa
from app.core.settings import settings

engine = sa.create_engine(settings.database_url, future=True)

metadata = sa.MetaData()

documents = sa.Table(
    "documents",
    metadata,
    sa.Column("id", sa.String, primary_key=True),
    sa.Column("filename", sa.String, nullable=False),
    sa.Column("status", sa.String, nullable=False),
)

def init_db():
    metadata.create_all(engine)
