from typing import Optional, Dict

from datetime import date, time, datetime
from sqlmodel import Field, SQLModel
from sqlalchemy import Column
from sqlalchemy_json import mutable_json_type
from pydantic import constr, conint, condecimal, AnyHttpUrl

from .base import DoLDataItem, CaseStatus


class SeasonalJobsJobOrder(DoLDataItem, table=True):
    """
    Job order scraped from SeasonalJobs.dol.gov
    """

    title: str
    link: Optional[AnyHttpUrl]
    description: Optional[str]
    dol_id: Optional[str]
    pub_date: Optional[str]
    scraped: bool = Field(default=False)
    scraped_data: Optional[Dict] = Field(sa_column=Column(mutable_json_type(nested=True)))
    pdf: Optional[str]

    def clean(self):
        # Check that the url field in scraped_data is not invalid.
        if not self.scraped_data:
            return
        apply_url = self.scraped_data.get("apply_url", "")
        if apply_url == "N/A":
            self.scraped_data["apply_url"] = ""
        elif "https://http:" in apply_url:
            self.scraped_data["apply_url"] = apply_url.replace(
                "https://http:", "https://"
            )