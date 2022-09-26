from sqlmodel import SQLModel

from db import get_engine
from models.address_record import AddressRecord  # noqa
from models.dedupe_blocking_map import DedupeBlockingMap  # noqa
from models.dedupe_entity_map import DedupeEntityMap  # noqa
from models.dol_disclosure_job_order import DolDisclosureJobOrder  # noqa
from models.dol_disclosure_job_order_address_record_link import (  # noqa
    DolDisclosureJobOrderAddressRecordLink,
)
from models.employer_record import EmployerRecord  # noqa
from models.employer_record_address_link import EmployerRecordAddressLink  # noqa
from models.seasonal_jobs_job_order import SeasonalJobsJobOrder  # noqa
from models.static_value import StaticValue  # noqa
from models.unique_employer import UniqueEmployer  # noqa
from settings import ALEMBIC_CONFIG_PATH


def initialize_db():
    SQLModel.metadata.create_all(get_engine())

    # then, load the Alembic configuration and generate the
    # version table, "stamping" it with the most recent rev:
    from alembic import command  # noqa
    from alembic.config import Config  # noqa

    alembic_cfg = Config(ALEMBIC_CONFIG_PATH)
    command.stamp(alembic_cfg, "head")


if __name__ == "__main__":
    initialize_db()
