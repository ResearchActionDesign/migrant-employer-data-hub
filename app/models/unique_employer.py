import uuid
from typing import TYPE_CHECKING, List, Optional

from pydantic import UUID4
from sqlmodel import JSON, Column, Field, Relationship

from .base import DoLDataItemWithoutSourceOrId, DoLDataSource

# Technique to avoid circular imports, see https://sqlmodel.tiangolo.com/tutorial/code-structure/
if TYPE_CHECKING:
    from app.models import EmployerRecord


class UniqueEmployer(DoLDataItemWithoutSourceOrId, table=True):
    """
    Record for a unique, deduped employer.
    """

    id: UUID4 = Field(
        default_factory=uuid.uuid4,
        primary_key=True,
        index=True,
        nullable=False,
    )
    sources: List[DoLDataSource] = Field(sa_column=Column(JSON))

    name: str
    trade_name_dba: Optional[str]
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]
    phone: Optional[str]

    # TODO: Populate with industry, NAICS code?

    # Relationships to other records
    employer_records: List["EmployerRecord"] = Relationship(  # noqa
        back_populates="unique_employer"
    )
