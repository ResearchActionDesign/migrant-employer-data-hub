from typing import Optional
from enum import Enum

from sqlmodel import SQLModel, Field
import sqlalchemy as sa

from datetime import datetime


class DoLDataSource(str, Enum):
    scraper = 'scraper'
    dol_disclosure = 'DoL annual or quarterly disclosure data'


class CaseStatus(str, Enum):
    certified = 'Determination Issued - Certification'
    withdrawn = 'Determination Issued - Withdrawn'
    denied = 'Determination Issued - Denied'
    partial = 'Determination Issued - Partial Certification'


class DoLDataItem(SQLModel):
    id: Optional[int] = Field(default=None, primary_key=True)
    source: DoLDataSource
    first_seen: Optional[datetime] = Field(sa_column=sa.Column(sa.DateTime, default=datetime.utcnow))
    last_seen: Optional[datetime] = Field(sa_column=sa.Column(sa.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow))


class StaticValue(SQLModel, table=True):
    """
    Static value storage in the DB for between runs.
    """

    key: str = Field(default=None, primary_key=True)
    value: Optional[str]

