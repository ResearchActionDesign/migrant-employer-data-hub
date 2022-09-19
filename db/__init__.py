from sqlmodel import SQLModel, create_engine

from settings import BASE_DIR

sqlite_file_name = "test_database.db"
sqlite_url = f"sqlite:///{BASE_DIR}/{sqlite_file_name}"


def get_engine(echo=False):
    engine = create_engine(sqlite_url, echo=echo)
    SQLModel.metadata.create_all(engine)
    return engine
