import hashlib
from datetime import datetime
from enum import Enum
from typing import Optional

from sqlmodel import Field, Relationship

from .base import DoLDataItem
from .employer_record import EmployerRecord


class AddressType(Enum):
    office = "Office Address"
    jobsite = "Jobsite Address"


class EmployerAddress(DoLDataItem, table=True):
    """
    Record for a unique employer address
    """

    employer_record_id: Optional[int] = Field(
        default=None, foreign_key="employer_record.id"
    )
    employer_record: Optional[EmployerRecord] = Relationship(back_populates="addresses")

    address_type: AddressType
    address_1: Optional[str]
    address_2: Optional[str]
    city: Optional[str]
    state: Optional[str]
    postal_code: Optional[str]
    country: Optional[str]

    # Geocoding info
    is_geocoded: bool = Field(default=False)
    geocoded_hash: Optional[
        str
    ]  # Hash of the address value at the time it was geocoded
    geocoded_date: Optional[datetime]
    lat: Optional[float]
    lon: Optional[float]

    def get_geocode_hash(self):
        return hashlib.md5(
            f"{self.address_1} {self.address_2}, {self.city}, {self.state} {self.postal_code} {self.country}".encode()
        )
