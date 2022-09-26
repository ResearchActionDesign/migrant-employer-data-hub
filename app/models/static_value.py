from typing import Optional

from sqlmodel import Field

from .base import SQLModelWithSnakeTableName


class StaticValue(SQLModelWithSnakeTableName, table=True):
    """
    Static value storage in the DB for between runs.
    """

    key: str = Field(default=None, primary_key=True)
    value: Optional[str]
