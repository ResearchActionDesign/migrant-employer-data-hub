from sqlmodel import SQLModel, create_engine, pool

from app.settings import DB_URL


def get_engine(echo=False, yield_per=False, refresh=False):
    if refresh or not hasattr(get_engine, "engine"):
        get_engine.engine = create_engine(
            DB_URL,
            echo=echo,
            execution_options=({"yield_per": yield_per} if yield_per else {}),
            connect_args={"sslmode": "require"},
        )
    SQLModel.metadata.create_all(get_engine.engine)
    return get_engine.engine


def get_mock_engine():
    from app.models.address_record import AddressRecord  # noqa
    from app.models.dedupe_entity_map import DedupeEntityMap  # noqa
    from app.models.dol_disclosure_job_order import DolDisclosureJobOrder  # noqa
    from app.models.employer_record import EmployerRecord  # noqa
    from app.models.employer_record_address_link import (  # noqa
        EmployerRecordAddressLink,
    )
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
