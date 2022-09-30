import hashlib
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional, Union

from sqlalchemy import exc, text
from sqlmodel import Field, Relationship, Session

from app.constants import US_STATE_ABBREVIATIONS, US_STATES_TO_ABBREV
from app.models.base import SQLModelWithSnakeTableName, clean_string_field
from app.models.dol_disclosure_job_order_address_record_link import (
    DolDisclosureJobOrderAddressRecordLink,
)
from app.settings import DB_ENGINE

# Technique to avoid circular imports, see https://sqlmodel.tiangolo.com/tutorial/code-structure/
if TYPE_CHECKING:
    from app.models.dol_disclosure_job_order import DolDisclosureJobOrder
    from app.models.employer_record_address_link import EmployerRecordAddressLink


def title_case_or_none(obj: Union[str, None]) -> Union[str, object]:
    if obj is not None:
        return str(obj).title()
    return None


def normalize_address(
    address_str: Union[str, None], session: Union[Session, None] = None
) -> str:
    if not address_str:
        return None

    if DB_ENGINE == "postgres" and session:
        try:
            result = session.exec(
                text(
                    f"select coalesce("
                    f"nullif("
                    f"pprint_addy("
                    f"pagc_normalize_address('{address_str}')), ''), '{address_str}') "
                    f"as address"
                )
            )
            return result.first()[0]
        except exc.DBAPIError as e:
            print(e)

    return title_case_or_none(address_str)


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
    address_1: Optional[str] = Field(index=True)
    address_2: Optional[str] = Field(index=True)
    normalized_address: Optional[str] = Field(
        index=True
    )  # Deduped/normalized version of address 1 + address 2.
    city: Optional[str] = Field(index=True)
    state: Optional[str] = Field(index=True)
    postal_code: Optional[str] = Field(index=True)
    country: Optional[str] = Field(index=True)

    # Geocoding-related fields
    is_geocoded: bool = Field(default=False)
    geocoded_hash: Optional[
        str
    ]  # Hash of the address value at the time it was geocoded
    geocoded_date: Optional[datetime]
    lat: Optional[float]
    lon: Optional[float]

    def __str__(self) -> str:
        address_part = " ".join([v for v in (self.address_1, self.address_2) if v])
        city_state = ", ".join([v for v in (self.city, self.state) if v])
        address_city_state = ", ".join([v for v in (address_part, city_state) if v])
        return " ".join(
            [v for v in (address_city_state, self.postal_code, self.country) if v]
        )

    def is_null(self) -> bool:
        return str(self).strip() == ""

    def get_geocode_hash(self) -> str:  # TODO: fix
        return hashlib.md5(str(self).encode())

    def clean(self) -> "AddressRecord":

        self.address_1 = (
            clean_string_field(self.address_1.title()) if self.address_1 else None
        )
        self.address_2 = (
            clean_string_field(self.address_2.title()) if self.address_2 else None
        )
        self.city = clean_string_field(self.city.title()) if self.city else None
        self.postal_code = clean_string_field(self.postal_code)
        self.state = clean_string_field(self.state).upper() if self.state else None
        self.country = (
            clean_string_field(self.country).upper() if self.country else None
        )

        if str(self.state).lower() in US_STATES_TO_ABBREV:
            self.state = US_STATES_TO_ABBREV[str(self.state).lower()].upper()

        if (
            self.country is None
            and self.state
            and self.state.lower() in US_STATE_ABBREVIATIONS
        ):
            self.country = "UNITED STATES OF AMERICA"

        self.normalized_address = normalize_address(str(self))

        return self
