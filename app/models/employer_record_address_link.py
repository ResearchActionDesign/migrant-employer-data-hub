from enum import Enum
from typing import TYPE_CHECKING, Optional

import sqlalchemy as sa
from sqlmodel import Field, Relationship

from app.models.address_record import AddressRecord

from .base import DoLDataItem

# Technique to avoid circular imports, see https://sqlmodel.tiangolo.com/tutorial/code-structure/
if TYPE_CHECKING:
    from app.models.employer_record import EmployerRecord


class AddressType(Enum):
    office = "Office Address"
    jobsite = "Jobsite Address"


class EmployerRecordAddressLink(DoLDataItem, table=True):
    """
    Stores links between an individual address record and an individual employer record.

    Employers <=> Addresses is a many-to-many relationship.

    This table also stores metadata related to the address at an employer-level.
    """

    # Override base fields
    id: Optional[int] = Field(
        sa_column=sa.Column("id", sa.Integer, autoincrement=True),
        default=None,
        primary_key=True,
    )

    # Data fields
    address_type: AddressType = Field(default=None, primary_key=True)

    # Relationship fields
    employer_record_id: Optional[int] = Field(
        default=None, foreign_key="employer_record.id", primary_key=True
    )
    address_record_id: Optional[int] = Field(
        default=None, foreign_key="address_record.id", primary_key=True
    )

    employer_record: Optional["EmployerRecord"] = Relationship(  # noqa
        back_populates="address_record_links"
    )
    address_record: Optional[AddressRecord] = Relationship(  # noqa
        back_populates="employer_record_links"
    )
