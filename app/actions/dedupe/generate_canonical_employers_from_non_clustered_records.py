from sqlalchemy import null
from sqlmodel import Session
from sqlmodel import select as sqlmodel_select

from app.db import get_engine
from app.models.dedupe_entity_map import DedupeEntityMap
from app.models.dol_disclosure_job_order import DolDisclosureJobOrder  # noqa
from app.models.employer_record import EmployerRecord
from app.models.unique_employer import UniqueEmployer
from app.settings import ROWS_BEFORE_COMMIT


def generate_canonical_employers_from_non_clustered_records() -> None:
    engine = get_engine()
    session = Session(engine)

    employers_with_no_cluster = session.exec(
        sqlmodel_select(EmployerRecord)
        .join(
            DedupeEntityMap,
            DedupeEntityMap.employer_record_id == EmployerRecord.id,
            isouter=True,
        )
        .where(DedupeEntityMap.employer_record_id == null())
        .where(EmployerRecord.unique_employer_id == null())
    )

    i = 0
    for e in employers_with_no_cluster:
        unique_employer = UniqueEmployer(
            name=e.name,
            trade_name_dba=e.trade_name_dba,
            city=e.city,
            state=e.state,
            country=e.country,
            phone=e.phone,
            last_seen=e.last_seen,
            first_seen=e.first_seen,
            sources=[
                e.source,
            ],
        )
        e.unique_employer = unique_employer
        session.add(unique_employer)
        session.add(e)
        i += 1

        if i % ROWS_BEFORE_COMMIT == 0:
            print(f"Added {i} new employers from non-clustered records.")
            session.commit()

    if i > 0:
        print(f"Added {i} new employers from non-clustered records.")
        session.commit()
    session.close()


if __name__ == "__main__":
    generate_canonical_employers_from_non_clustered_records()
