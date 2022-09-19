from typing import List, Optional

from sqlmodel import Field, Relationship

from .base import DoLDataItem


class EmployerRecord(DoLDataItem, table=True):
    """
    Record for a unique employer.
    """

    name: str = Field(index=True)
    trade_name_dba: Optional[str]
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]
    phone: Optional[str]

    # Dedupe UUID
    employer_uuid: Optional[str] = Field(default=None, index=True)

    # Relationships to other records
    dol_disclosure_job_orders: List["DolDisclosureJobOrder"] = Relationship(  # noqa
        back_populates="employer_record"
    )
    seasonal_jobs_job_orders: List["SeasonalJobsJobOrder"] = Relationship(  # noqa
        back_populates="employer_record"
    )
    addresses: List["EmployerAddress"] = Relationship(  # noqa
        back_populates="employer_record"
    )
