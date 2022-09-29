from sqlmodel import Session, select

from app.db import get_engine
from app.models.dol_disclosure_job_order import DolDisclosureJobOrder
from app.models.seasonal_jobs_job_order import SeasonalJobsJobOrder


def reclean_raw_listings():
    """
    Iterate through all the raw listings in the DB and clean each one.

    :return:
    """

    session = Session(get_engine())
    dol_disclosures = session.exec(select(DolDisclosureJobOrder))

    for d in dol_disclosures:
        d.clean()
        session.add(d)

    session.commit()

    seasonal_jobs = session.exec(select(SeasonalJobsJobOrder))

    for s in seasonal_jobs:
        s.clean()
        session.add(s)

    session.commit()
