from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool
# @see https://github.com/tiangolo/sqlmodel/issues/85
from sqlmodel import SQLModel

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Model / Schema imports
from models.address_record import AddressRecord  # noqa
from models.dedupe_blocking_map import DedupeBlockingMap  # noqa
from models.dedupe_entity_map import DedupeEntityMap  # noqa
from models.dol_disclosure_job_order import DolDisclosureJobOrder  # noqa
from models.employer_record import EmployerRecord  # noqa
from models.employer_record_address_link import EmployerRecordAddressLink  # noqa
from models.seasonal_jobs_job_order import SeasonalJobsJobOrder  # noqa
from models.static_value import StaticValue  # noqa
from models.unique_employer import UniqueEmployer  # noqa

target_metadata = SQLModel.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


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
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

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
