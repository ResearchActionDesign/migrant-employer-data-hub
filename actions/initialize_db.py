from sqlmodel import SQLModel

from db import get_engine
from models.address_record import AddressRecord  # noqa
from models.dol_disclosure_job_order import DolDisclosureJobOrder  # noqa
from models.dol_disclosure_job_order_address_record_link import (  # noqa
    DolDisclosureJobOrderAddressRecordLink,
)
from models.employer_record import EmployerRecord  # noqa
from models.employer_record_address_link import EmployerRecordAddressLink  # noqa
from models.seasonal_jobs_job_order import SeasonalJobsJobOrder  # noqa
from models.static_value import StaticValue  # noqa


def initialize_db():
    SQLModel.metadata.create_all(get_engine(echo=True))


if __name__ == "__main__":
    initialize_db()
