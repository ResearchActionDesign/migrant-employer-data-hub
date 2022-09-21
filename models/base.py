import re
from datetime import datetime
from enum import Enum
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.orm import declared_attr
from sqlmodel import Field, SQLModel


def clean_string_field(value: str | None) -> str | None:
    if not value:
        return None
    value = re.sub("  +", " ", value)
    value = re.sub("\n", " ", value)
    value = value.strip().strip('"').strip("'").strip()

    if value.lower() == "n/a":
        value = None

    if not value:
        value = None
    return value


class DoLDataSource(str, Enum):
    scraper = "scraper"
    dol_disclosure = "DoL annual or quarterly disclosure data"


class CaseStatus(str, Enum):
    certified = "Determination Issued - Certification"
    withdrawn = "Determination Issued - Withdrawn"
    denied = "Determination Issued - Denied"
    partial = "Determination Issued - Partial Certification"


class SQLModelWithSnakeTableName(SQLModel):
    @declared_attr  # type: ignore
    def __tablename__(cls) -> str:  # noqa
        return re.sub(r"(?<!^)(?=[A-Z])", "_", cls.__name__).lower()


class DoLDataItem(SQLModelWithSnakeTableName):
    id: Optional[int] = Field(default=None, primary_key=True)
    source: DoLDataSource
    first_seen: Optional[datetime] = Field(
        sa_column=sa.Column(sa.DateTime, default=datetime.utcnow)
    )
    last_seen: Optional[datetime] = Field(
        sa_column=sa.Column(
            sa.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
        )
    )
