from typing import Dict, Optional

from pydantic import AnyHttpUrl, constr
from sqlalchemy import Column
from sqlalchemy_json import mutable_json_type
from sqlmodel import Field, Relationship

from constants import US_STATES_TO_ABBREV

from .base import DoLDataItem, DoLDataSource
from .employer_record import EmployerRecord


class SeasonalJobsJobOrder(DoLDataItem, table=True):
    """
    Job order scraped from SeasonalJobs.dol.gov
    """

    # Relationship fields
    employer_record_id: Optional[int] = Field(
        default=None, foreign_key="employer_record.id"
    )
    employer_record: Optional[EmployerRecord] = Relationship(
        back_populates="seasonal_jobs_job_orders"
    )

    # Override base fields
    source = Field(default=DoLDataSource.scraper)

    # Data fields
    title: str = Field(index=True)
    link: Optional[AnyHttpUrl]
    description: Optional[str]
    dol_id: Optional[str]
    pub_date: Optional[str]
    scraped: bool = Field(default=False)
    scraped_data: Optional[Dict] = Field(
        sa_column=Column(mutable_json_type(nested=True))
    )
    pdf: Optional[str]

    employer_name: Optional[str] = Field(index=True)
    trade_name_dba: Optional[str]
    employer_city: Optional[str]
    employer_state: Optional[str]
    employer_postal_code: Optional[constr(max_length=10)]
    employer_phone: Optional[str]

    # This field isn't exposed on the seasonaljobs listings, but we need it for dedupe.
    employer_country: Optional[str] = Field(default="UNITED STATES OF AMERICA")

    def clean(self):
        # Check that the url field in scraped_data is not invalid.
        if not self.scraped_data:
            return self

        apply_url = self.scraped_data.get("apply_url", "")
        if apply_url == "N/A":
            self.scraped_data["apply_url"] = ""
        elif "https://http:" in apply_url:
            self.scraped_data["apply_url"] = apply_url.replace(
                "https://http:", "https://"
            )

        # Update employer fields from JSON
        employer_fields = {
            "employer_business_name": "employer_name",
            "employer_trade_name": "trade_name_dba",
            "employer_city": "employer_city",
            "employer_state": "employer_state",
            "employer_zip": "employer_postal_code",
            "employer_phone": "employer_phone",
        }
        for field_name, mapped_field_name in employer_fields.items():
            v = self.scraped_data.get(field_name, None)
            if v is not None:
                v = v.strip()
            if v == "":
                v = None
            setattr(self, mapped_field_name, v)

        if str(self.employer_state).lower() in US_STATES_TO_ABBREV:
            self.employer_state = US_STATES_TO_ABBREV[
                str(self.employer_state).lower()
            ].upper()

        return self
