from sqlalchemy import Column, Table
from sqlalchemy import TIMESTAMP, TEXT
from .schema import metadata

peer_table = Table(
    "oban_peers",
    metadata,
    Column("name", TEXT, primary_key=True),
    Column("node", TEXT, nullable=False),
    Column("started_at", TIMESTAMP, nullable=False),
    Column("expires_at", TIMESTAMP, nullable=False),
)
