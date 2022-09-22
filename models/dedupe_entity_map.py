from datetime import datetime
from typing import Optional

from sqlmodel import Field

from .base import SQLModelWithSnakeTableName


class DedupeEntityMap(SQLModelWithSnakeTableName, table=True):
    """
    SQLModel model for the dedupe entity map because we want to keep this around.
    """

    employer_record_id: Optional[int] = Field(
        default=None, primary_key=True
    )  # Note this is actually a foreign key to employer_record.
    canon_id: Optional[int] = Field(default=None, index=True)
    cluster_score: Optional[float]
    is_valid_cluster: Optional[bool]
    review_date: Optional[datetime]
    processed_to_canonical_employer: Optional[bool] = Field(default=False)
