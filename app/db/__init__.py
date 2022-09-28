import json
import os

import boto3
from sqlalchemy.future import Engine
from sqlmodel import SQLModel, create_engine, pool

from app.settings import DB_ENGINE, DB_URL, ENVIRONMENT


def get_aws_db_credentials() -> dict:
    secret_name = "stage/CDMDataHub/lambda_user"
    region_name = "us-east-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name=region_name,
    )

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    if "SecretString" in get_secret_value_response:
        return json.loads(get_secret_value_response["SecretString"])
    return {}


if ENVIRONMENT == "lambda" and DB_ENGINE == "postgres":
    db_credentials = get_aws_db_credentials()
    DB_URL = (  # noqa
        f"postgresql://{db_credentials['username']}:"
        f"{db_credentials['password']}"
        f"@{db_credentials['host']}:{db_credentials['port']}/{os.getenv('POSTGRES_DB', 'postgres')}"
    )

elif DB_ENGINE == "postgres":
    DB_URL = (  # noqa
        f"postgresql://{os.getenv('POSTGRES_USER', 'postgres')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'postgres')}"
        f"@{os.getenv('POSTGRES_URL', '0.0.0.0')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB','postgres')}"
    )


def get_engine(echo=False, yield_per=False, refresh=False) -> Engine:
    if refresh or not hasattr(get_engine, "engine"):
        get_engine.engine = create_engine(
            DB_URL,
            echo=echo,
            execution_options=({"yield_per": yield_per} if yield_per else {}),
            connect_args=({"sslmode": "require"} if DB_ENGINE == "postgres" else {}),
        )
    SQLModel.metadata.create_all(get_engine.engine)
    return get_engine.engine


def get_mock_engine() -> Engine:
    from app.models.address_record import AddressRecord  # noqa
    from app.models.dedupe_entity_map import DedupeEntityMap  # noqa
    from app.models.dol_disclosure_job_order import DolDisclosureJobOrder  # noqa
    from app.models.employer_record import EmployerRecord  # noqa
    from app.models.employer_record_address_link import (  # noqa
        EmployerRecordAddressLink,
    )
    from app.models.imported_dataset import ImportedDataset  # noqa
    from app.models.seasonal_jobs_job_order import SeasonalJobsJobOrder  # noqa
    from app.models.static_value import StaticValue  # noqa
    from app.models.unique_employer import UniqueEmployer  # noqa

    if not hasattr(get_mock_engine, "engine"):
        get_mock_engine.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=pool.StaticPool,
            echo=False,
        )

    SQLModel.metadata.create_all(get_mock_engine.engine)
    return get_mock_engine.engine


def drop_all_models():
    engine = get_mock_engine()
    SQLModel.metadata.drop_all(engine)
