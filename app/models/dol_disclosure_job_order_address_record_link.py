from typing import Optional

from sqlmodel import Field

from .base import SQLModelWithSnakeTableName


class DolDisclosureJobOrderAddressRecordLink(SQLModelWithSnakeTableName, table=True):
    """
    Many-to-many link table to associate each address record with all the source
    disclosure data rows where it appears. Mainly for audit log purposes.
    """

    dol_disclosure_job_order_id: Optional[int] = Field(
        default=None, foreign_key="dol_disclosure_job_order.id", primary_key=True
    )
    address_record_id: Optional[int] = Field(
        default=None, foreign_key="address_record.id", primary_key=True
    )
