from sqlmodel import SQLModel

from app.db import get_engine
from app.models.address_record import AddressRecord  # noqa
from app.models.dedupe_blocking_map import DedupeBlockingMap  # noqa
from app.models.dedupe_entity_map import DedupeEntityMap  # noqa
from app.models.dol_disclosure_job_order import DolDisclosureJobOrder  # noqa
from app.models.dol_disclosure_job_order_address_record_link import (  # noqa
    DolDisclosureJobOrderAddressRecordLink,
)
from app.models.employer_record import EmployerRecord  # noqa
from app.models.employer_record_address_link import EmployerRecordAddressLink  # noqa
from app.models.imported_dataset import ImportedDataset  # noqa
from app.models.seasonal_jobs_job_order import SeasonalJobsJobOrder  # noqa
from app.models.static_value import StaticValue  # noqa
from app.models.unique_employer import UniqueEmployer  # noqa
from app.settings import ALEMBIC_CONFIG_PATH


def initialize_db() -> None:
    SQLModel.metadata.create_all(get_engine())

    # then, load the Alembic configuration and generate the
    # version table, "stamping" it with the most recent rev:
    from alembic import command  # noqa
    from alembic.config import Config  # noqa

    alembic_cfg = Config(ALEMBIC_CONFIG_PATH)
    command.stamp(alembic_cfg, "head")


if __name__ == "__main__":
    initialize_db()
