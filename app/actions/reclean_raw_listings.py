from sqlmodel import Session, select

from app.db import get_engine
from app.models.dol_disclosure_job_order import DolDisclosureJobOrder
from app.models.seasonal_jobs_job_order import SeasonalJobsJobOrder
from app.settings import ROWS_BEFORE_COMMIT


def reclean_raw_listings():
    """
    Iterate through all the raw listings in the DB and clean each one.

    :return:
    """

    session = Session(get_engine())
    dol_disclosures = session.exec(select(DolDisclosureJobOrder))

    count = 0
    for d in dol_disclosures:
        d.clean()
        count += 1
        session.add(d)

        if count % ROWS_BEFORE_COMMIT == 0:
            print(f"Cleaned {count} dol_disclosure_items")
            session.commit()

    session.commit()

    seasonal_jobs = session.exec(select(SeasonalJobsJobOrder))

    count = 0
    for s in seasonal_jobs:
        s.clean()
        count += 1
        session.add(s)

        if count % ROWS_BEFORE_COMMIT == 0:
            print(f"Cleaned {count} seasonal_jobs items")
            session.commit()

    session.commit()


if __name__ == "__main__":
    reclean_raw_listings()
