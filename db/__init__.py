from sqlmodel import SQLModel, create_engine, pool

from settings import BASE_DIR

sqlite_file_name = "test_database.db"
sqlite_url = f"sqlite:///{BASE_DIR}/{sqlite_file_name}"


def get_engine(echo=False):
    engine = create_engine(sqlite_url, echo=echo)
    SQLModel.metadata.create_all(engine)
    return engine


def get_mock_engine():
    from models.address_record import AddressRecord  # noqa
    from models.dol_disclosure_job_order import DolDisclosureJobOrder  # noqa
    from models.employer_record import EmployerRecord  # noqa
    from models.employer_record_address_link import EmployerRecordAddressLink  # noqa
    from models.seasonal_jobs_job_order import SeasonalJobsJobOrder  # noqa

    if not hasattr(get_mock_engine, "engine"):
        get_mock_engine.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": True},
            poolclass=pool.StaticPool,
            echo=False,
        )

    SQLModel.metadata.create_all(get_mock_engine.engine)
    return get_mock_engine.engine


def drop_all_models():
    engine = get_mock_engine()
    SQLModel.metadata.drop_all(engine)
