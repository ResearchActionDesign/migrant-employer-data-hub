from datetime import datetime
from enum import Enum
from typing import Optional

import sqlalchemy as sa
from sqlmodel import Field

from app.models.base import SQLModelWithSnakeTableName


class ImportStatus(str, Enum):
    finished = "Finished"
    import_running = "Import Running - Locked"  # Currently unused.
    needs_importing = "Needs Importing"


class ImportedDataset(SQLModelWithSnakeTableName, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    bucket_name: Optional[str]
    object_name: Optional[str]
    import_status: Optional[ImportStatus] = Field(default=ImportStatus.needs_importing)
    created: Optional[datetime] = Field(
        sa_column=sa.Column(sa.DateTime, default=datetime.utcnow)
    )
    modified: Optional[datetime] = Field(
        sa_column=sa.Column(
            sa.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
        )
    )
