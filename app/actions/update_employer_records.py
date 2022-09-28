from sqlalchemy import text
from sqlmodel import Session

from app.db import get_engine
from app.settings import DB_ENGINE

# flake8: noqa

IS_ = "IS" if DB_ENGINE == "sqlite" else "IS NOT DISTINCT FROM"


def update_employer_records_from_disclosure_data(session: Session):
    # First, pull unique sets of employer name, trade name, etc.
    # from the disclosure records and assign those to new employer
    # records if there aren't already matching employer records.
    sql_query = f"""
        INSERT INTO employer_record(first_seen, last_seen, source, name, trade_name_dba, city, state, country, phone)
    select min(d.first_seen),
           max(d.last_seen),
           max(d.source),
           max(d.employer_name)    as employer_name,
           max(d.trade_name_dba)   as trade_name_dba,
           max(d.employer_city)    as employer_city,
           max(d.employer_state)   as employer_state,
           max(d.employer_country) as employer_country,
           max(d.employer_phone)        as employer_phone
    from dol_disclosure_job_order d
             join (SELECT DISTINCT lower(dol.employer_name)    as employer_name,
                                   lower(dol.trade_name_dba)   as trade_name_dba,
                                   lower(dol.employer_city)    as employer_city,
                                   lower(dol.employer_state)   as employer_state,
                                   lower(dol.employer_country) as employer_country,
                                   dol.employer_phone          as employer_phone
                   FROM dol_disclosure_job_order dol
                            left outer join employer_record e
                                            on lower(dol.employer_name) {IS_} lower(e.name) and
                                               lower(dol.trade_name_dba) {IS_} lower(e.trade_name_dba) and
                                               lower(dol.employer_city) {IS_} lower(e.city) and
                                               lower(dol.employer_state) {IS_} lower(e.state) and
                                               lower(dol.employer_country) {IS_} lower(e.country) and
                                               dol.employer_phone {IS_} e.phone
                   where e.name is null and dol.employer_record_id is null) u
                  on lower(d.employer_name) {IS_} u.employer_name and lower(d.trade_name_dba) {IS_} u.trade_name_dba and
                     lower(d.employer_city) {IS_} u.employer_city and lower(d.employer_state) {IS_} u.employer_state and
                     lower(d.employer_country) {IS_} u.employer_country and d.employer_phone {IS_} u.employer_phone

    group by u.employer_name, u.trade_name_dba, u.employer_city, u.employer_state, u.employer_country, u.employer_phone;
        """

    session.exec(text(sql_query))

    # Next, update the linked employer_record_ids.
    sql_query = f"""update dol_disclosure_job_order
    set employer_record_id = (
    select id from employer_record e
    where lower(dol_disclosure_job_order.employer_name) {IS_} lower(e.name)
      and lower(dol_disclosure_job_order.trade_name_dba) {IS_} lower(e.trade_name_dba)
      and lower(dol_disclosure_job_order.employer_city) {IS_} lower(e.city)
      and lower(dol_disclosure_job_order.employer_state) {IS_} lower(e.state)
      and lower(dol_disclosure_job_order.employer_country) {IS_} lower(e.country)
      and dol_disclosure_job_order.employer_phone {IS_} e.phone)
      where dol_disclosure_job_order.employer_record_id is null;"""
    session.exec(text(sql_query))

    # Lastly, update the last_seen value for employers if needed.
    sql_query = """update employer_record
    set last_seen = (select max(last_seen)
    from dol_disclosure_job_order where employer_record_id = employer_record.id group by employer_record_id)
    where exists (select last_seen from dol_disclosure_job_order where employer_record_id = employer_record.id and last_seen > employer_record.last_seen);"""
    session.exec(text(sql_query))


def update_employer_records_from_seasonal_jobs(session: Session):
    # First, pull unique sets of employer name, trade name, etc.
    # from the disclosure records and assign those to new employer
    # records if there aren't already matching employer records.
    sql_query = f"""
          INSERT INTO employer_record(first_seen, last_seen, source, name, trade_name_dba, city, state, country, phone)
            select min(s.first_seen),
                   max(s.last_seen),
                   max(s.source),
                   max(s.employer_name)    as employer_name,
                   max(s.trade_name_dba)   as trade_name_dba,
                   max(s.employer_city)    as employer_city,
                   max(s.employer_state)   as employer_state,
                   max(s.employer_country) as employer_country,
                   max(s.employer_phone)        as employer_phone
            from seasonal_jobs_job_order s
                     join (SELECT DISTINCT lower(seasonal.employer_name)    as employer_name,
                                           lower(seasonal.trade_name_dba)   as trade_name_dba,
                                           lower(seasonal.employer_city)    as employer_city,
                                           lower(seasonal.employer_state)   as employer_state,
                                           lower(seasonal.employer_country) as employer_country,
                                           seasonal.employer_phone          as employer_phone
                           FROM seasonal_jobs_job_order seasonal
                                    left outer join employer_record e
                                                    on lower(seasonal.employer_name) {IS_} lower(e.name) and
                                                       lower(seasonal.trade_name_dba) {IS_} lower(e.trade_name_dba) and
                                                       lower(seasonal.employer_city) {IS_} lower(e.city) and
                                                       lower(seasonal.employer_state) {IS_} lower(e.state) and
                                                       lower(seasonal.employer_country) {IS_} lower(e.country) and
                                                       seasonal.employer_phone {IS_} e.phone
                           where seasonal.scraped and e.name is null and seasonal.employer_record_id is null) u
                          on lower(s.employer_name) {IS_} u.employer_name and lower(s.trade_name_dba) {IS_} u.trade_name_dba and
                             lower(s.employer_city) {IS_} u.employer_city and lower(s.employer_state) {IS_} u.employer_state and
                             lower(s.employer_country) {IS_} u.employer_country and s.employer_phone {IS_} u.employer_phone
            group by u.employer_name, u.trade_name_dba, u.employer_city, u.employer_state, u.employer_country, u.employer_phone;
            """

    session.exec(text(sql_query))

    # Next, update the linked employer_record_ids.
    sql_query = f"""update seasonal_jobs_job_order
        set employer_record_id = (select id
         from employer_record e
        where lower(seasonal_jobs_job_order.employer_name) {IS_} lower(e.name)
          and lower(seasonal_jobs_job_order.trade_name_dba) {IS_} lower(e.trade_name_dba)
          and lower(seasonal_jobs_job_order.employer_city) {IS_} lower(e.city)
          and lower(seasonal_jobs_job_order.employer_state) {IS_} lower(e.state)
          and lower(seasonal_jobs_job_order.employer_country) {IS_} lower(e.country)
          and seasonal_jobs_job_order.employer_phone {IS_} e.phone)
          where seasonal_jobs_job_order.employer_record_id is null;"""
    session.exec(text(sql_query))

    # Lastly, update the last_seen value for employers if needed.
    sql_query = """update employer_record
    set last_seen = (select max(last_seen)
    from seasonal_jobs_job_order where employer_record_id = employer_record.id group by employer_record_id)
    where exists (select last_seen from seasonal_jobs_job_order where employer_record_id = employer_record.id and last_seen > employer_record.last_seen);"""
    session.exec(text(sql_query))


def update_employer_records() -> bool:
    """
    Find new unique employer name/city/state/phone combos and add then to the employer records list.
    """

    # Parse through DoL disclosure data table and add any new employers there to the employer records table
    session = Session(get_engine())
    update_employer_records_from_disclosure_data(session)
    session.commit()

    # Now do the same for the scraped seasonal job order data.
    update_employer_records_from_seasonal_jobs(session)
    session.commit()
    session.close()
    return True


if __name__ == "__main__":
    update_employer_records()
