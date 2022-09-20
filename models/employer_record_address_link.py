from enum import Enum
from typing import Optional

from sqlmodel import Field, Relationship

from .base import DoLDataItem


class AddressType(Enum):
    office = "Office Address"
    jobsite = "Jobsite Address"


class EmployerRecordAddressLink(DoLDataItem, table=True):
    """
    Stores links between an individual address record and an individual employer record.

    Employers <=> Addresses is a many-to-many relationship.

    This table also stores metadata related to the address at an employer-level.
    """

    # Data fields
    address_type: AddressType = Field(default=None, primary_key=True)

    # Relationship fields
    employer_record_id: Optional[int] = Field(
        default=None, foreign_key="employer_record.id", primary_key=True
    )
    address_record_id: Optional[int] = Field(
        default=None, foreign_key="address_record.id", primary_key=True
    )

    employer_record: "EmployerRecord" = Relationship(  # noqa
        back_populates="address_record_links"
    )
    address_record: "AddressRecord" = Relationship(  # noqa
        back_populates="employer_record_links"
    )
