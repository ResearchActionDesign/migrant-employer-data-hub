from sqlmodel import SQLModel, Session, create_engine
from settings import BASE_DIR

sqlite_file_name = 'test_database.db'
sqlite_url = f"sqlite:///{BASE_DIR}/{sqlite_file_name}"

engine = create_engine(sqlite_url, echo=True)

SQLModel.metadata.create_all(engine)
