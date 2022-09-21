import hashlib
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from sqlmodel import Field, Relationship

from constants import US_STATES_TO_ABBREV
from models.base import SQLModelWithSnakeTableName
from models.dol_disclosure_job_order_address_record_link import (
    DolDisclosureJobOrderAddressRecordLink,
)

# Technique to avoid circular imports, see https://sqlmodel.tiangolo.com/tutorial/code-structure/
if TYPE_CHECKING:
    from models.dol_disclosure_job_order import DolDisclosureJobOrder
    from models.employer_record_address_link import EmployerRecordAddressLink


class AddressRecord(SQLModelWithSnakeTableName, table=True):
    """
    Record for a unique address.

    One address record could be linked to multiple employers. The
    EmployerRecordAddressLink class stores metadata like first_seen, last_seen, source
    since this is associated with the individual appearance of each address with each employer.
    """

    id: Optional[int] = Field(default=None, primary_key=True)

    # Relationship fields
    employer_record_links: List["EmployerRecordAddressLink"] = Relationship(
        back_populates="address_record",
    )
    dol_disclosure_job_orders: List["DolDisclosureJobOrder"] = Relationship(  # noqa
        back_populates="address_records",
        link_model=DolDisclosureJobOrderAddressRecordLink,
    )

    # Data fields. Note -- string fields here are case-converted before saving, for consistency.
    address_1: Optional[str]
    address_2: Optional[str]
    city: Optional[str]
    state: Optional[str]
    postal_code: Optional[str]
    country: Optional[str]

    # Geocoding-related fields
    is_geocoded: bool = Field(default=False)
    geocoded_hash: Optional[
        str
    ]  # Hash of the address value at the time it was geocoded
    geocoded_date: Optional[datetime]
    lat: Optional[float]
    lon: Optional[float]

    def __str__(self):
        values_dict = [
            self.address_1,
            self.address_2,
            self.city + "," if self.city else None,
            self.state,
            self.postal_code,
            self.country,
        ]
        return " ".join([v for v in values_dict if v]).strip()

    def is_null(self):
        return str(self).strip() == ""

    def get_geocode_hash(self):
        return hashlib.md5(str(self).encode())

    def clean(self):

        self.address_1 = self.address_1.title().strip() if self.address_1 else None
        self.address_2 = self.address_2.title().strip() if self.address_2 else None
        self.city = self.city.title().strip() if self.city else None
        self.postal_code = self.postal_code.strip() if self.postal_code else None
        self.state = self.state.upper().strip() if self.state else None
        self.country = self.country.upper().strip() if self.country else None

        if str(self.state).lower() in US_STATES_TO_ABBREV:
            self.state = US_STATES_TO_ABBREV[str(self.state).lower()].upper()

        return self
