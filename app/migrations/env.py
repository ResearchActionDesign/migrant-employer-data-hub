from logging.config import fileConfig

from alembic import context
from alembic.script import write_hooks
# @see https://github.com/tiangolo/sqlmodel/issues/85
from sqlmodel import SQLModel

from app.db import get_engine

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Model / Schema imports
from app.models.address_record import AddressRecord  # noqa
from app.models.dedupe_blocking_map import DedupeBlockingMap  # noqa
from app.models.dedupe_entity_map import DedupeEntityMap  # noqa
from app.models.dol_disclosure_job_order import DolDisclosureJobOrder  # noqa
from app.models.employer_record import EmployerRecord  # noqa
from app.models.employer_record_address_link import EmployerRecordAddressLink  # noqa
from app.models.imported_dataset import ImportedDataset  # noqa
from app.models.seasonal_jobs_job_order import SeasonalJobsJobOrder  # noqa
from app.models.static_value import StaticValue  # noqa
from app.models.unique_employer import UniqueEmployer  # noqa

target_metadata = SQLModel.metadata


# Hook to add sqlmodel dependency to auto-generated migrations
@write_hooks.register("add_sqlmodel")
def add_sqlmodel(filename, options):
    lines = []
    with open(filename) as file_:
        for line in file_:
            lines.append(line)
            if line.startswith('import sqlalchemy as sa'):
                lines.append('import sqlmodel')
    with open(filename, "w") as to_write:
        to_write.write("".join(lines))


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = get_engine()

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
