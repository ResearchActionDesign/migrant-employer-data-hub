from typing import Optional

from sqlmodel import Field, Index

from .base import SQLModelWithSnakeTableName


class DedupeBlockingMap(SQLModelWithSnakeTableName, table=True):
    """
    SQLModel model for the dedupe blocking map because we want to keep this around in alembic migrations.
    """

    id: int = Field(default=None, primary_key=True)
    block_key: Optional[str]
    employer_record_id: Optional[int]


blocking_map_idx = Index(
    "blocking_map_idx",
    DedupeBlockingMap.block_key,
    DedupeBlockingMap.employer_record_id,
    postgresql_ops={"block_key": "text_pattern_ops", "employer_record_id": "int4_ops"},
)
