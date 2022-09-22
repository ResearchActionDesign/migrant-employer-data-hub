from typing import TYPE_CHECKING, List, Optional

from pydantic import UUID4
from sqlmodel import Field, Relationship

from models.employer_record_address_link import EmployerRecordAddressLink
from models.seasonal_jobs_job_order import SeasonalJobsJobOrder
from models.unique_employer import UniqueEmployer

from .base import DoLDataItem

# Technique to avoid circular imports, see https://sqlmodel.tiangolo.com/tutorial/code-structure/
if TYPE_CHECKING:
    from models.dol_disclosure_job_order import DolDisclosureJobOrder


class EmployerRecord(DoLDataItem, table=True):
    """
    Record for a unique combination of name, trade_name_dba, city, state.

    Note -- this type **does not** represent a unique employer.
    """

    name: str = Field(index=True)
    trade_name_dba: Optional[str]
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]
    phone: Optional[str]

    # Relationships to other records
    unique_employer_id: Optional[UUID4] = Field(
        default=None, foreign_key="unique_employer.id"
    )
    unique_employer: Optional[UniqueEmployer] = Relationship(
        back_populates="employer_records"
    )

    dol_disclosure_job_orders: List["DolDisclosureJobOrder"] = Relationship(  # noqa
        back_populates="employer_record"
    )
    seasonal_jobs_job_orders: List[SeasonalJobsJobOrder] = Relationship(  # noqa
        back_populates="employer_record"
    )
    address_record_links: List[EmployerRecordAddressLink] = Relationship(  # noqa
        back_populates="employer_record",
    )
