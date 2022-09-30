from sqlalchemy import text
from sqlalchemy.future import Engine
from sqlmodel import Session, SQLModel

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
from app.settings import ALEMBIC_CONFIG_PATH, DB_ENGINE

# flake8: noqa


def set_up_extensions(engine: Engine):
    sql_query = """CREATE EXTENSION IF NOT EXISTS "unaccent";

    CREATE OR REPLACE FUNCTION slugify("value" TEXT)
    RETURNS TEXT AS $$
      -- removes accents (diacritic signs) from a given string --
      WITH "unaccented" AS (
        SELECT unaccent("value") AS "value"
      ),
      -- lowercases the string
      "lowercase" AS (
        SELECT lower("value") AS "value"
        FROM "unaccented"
      ),
      -- replaces anything that's not a letter, number, hyphen('-'), or underscore('_') with a hyphen('-')
      "hyphenated" AS (
        SELECT regexp_replace("value", '[^a-z0-9\-_]+', '-', 'gi') AS "value"
        FROM "lowercase"
      ),
      -- trims hyphens('-') if they exist on the head or tail of the string
      "trimmed" AS (
        SELECT regexp_replace(regexp_replace("value", '\-+$', ''), '^\-', '') AS "value"
        FROM "hyphenated"
      )
      SELECT "value" FROM "trimmed";
    $$ LANGUAGE SQL STRICT IMMUTABLE;

    CREATE EXTENSION postgis;
    CREATE EXTENSION fuzzystrmatch;
    CREATE EXTENSION postgis_tiger_geocoder;
    CREATE EXTENSION address_standardizer;
    """

    if DB_ENGINE == "postgres":
        session = Session(engine)
        session.exec(text(sql_query))


def initialize_db() -> None:
    engine = get_engine()
    SQLModel.metadata.create_all(engine)

    # then, load the Alembic configuration and generate the
    # version table, "stamping" it with the most recent rev:
    from alembic import command  # noqa
    from alembic.config import Config  # noqa

    alembic_cfg = Config(ALEMBIC_CONFIG_PATH)
    command.stamp(alembic_cfg, "head")

    set_up_extensions(engine)


if __name__ == "__main__":
    initialize_db()
