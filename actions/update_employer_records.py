from sqlalchemy import text
from sqlmodel import Session

from db import get_engine

# flake8: noqa


def update_employer_records_from_disclosure_data(session: Session):
    # First, pull unique sets of employer name, trade name, etc.
    # from the disclosure records and assign those to new employer
    # records if there aren't already matching employer records.
    sql_query = """
        INSERT INTO employer_record(first_seen, last_seen, source, name, trade_name_dba, city, state, country, phone)
    select min(d.first_seen),
           max(d.last_seen),
           max(d.source),
           max(d.employer_name)    as employer_name,
           max(d.trade_name_dba)   as trade_name_dba,
           max(d.employer_city)    as employer_city,
           max(d.employer_state)   as employer_state,
           max(d.employer_country) as employer_country,
           d.employer_phone        as employer_phone
    from dol_disclosure_job_order d
             join (SELECT DISTINCT lower(dol.employer_name)    as employer_name,
                                   lower(dol.trade_name_dba)   as trade_name_dba,
                                   lower(dol.employer_city)    as employer_city,
                                   lower(dol.employer_state)   as employer_state,
                                   lower(dol.employer_country) as employer_country,
                                   dol.employer_phone          as employer_phone
                   FROM dol_disclosure_job_order dol
                            left outer join employer_record e
                                            on lower(dol.employer_name) IS lower(e.name) and
                                               lower(dol.trade_name_dba) IS lower(e.trade_name_dba) and
                                               lower(dol.employer_city) IS lower(e.city) and
                                               lower(dol.employer_state) IS lower(e.state) and
                                               lower(dol.employer_country) IS lower(e.country) and
                                               dol.employer_phone IS e.phone
                   where e.name is null and dol.employer_record_id is null) u
                  on lower(d.employer_name) IS u.employer_name and lower(d.trade_name_dba) IS u.trade_name_dba and
                     lower(d.employer_city) IS u.employer_city and lower(d.employer_state) IS u.employer_state and
                     lower(d.employer_country) IS u.employer_country and d.employer_phone IS u.employer_phone

    group by u.employer_name, u.trade_name_dba, u.employer_city, u.employer_state, u.employer_country, u.employer_phone;
        """

    session.exec(text(sql_query))

    # Next, update the linked employer_record_ids.
    sql_query = """update dol_disclosure_job_order
    set employer_record_id = e.id
    from (select id, name, trade_name_dba, city, state, country, phone from employer_record) as e
    where lower(dol_disclosure_job_order.employer_name) IS lower(e.name)
      and lower(dol_disclosure_job_order.trade_name_dba) IS lower(e.trade_name_dba)
      and lower(dol_disclosure_job_order.employer_city) IS lower(e.city)
      and lower(dol_disclosure_job_order.employer_state) IS lower(e.state)
      and lower(dol_disclosure_job_order.employer_country) IS lower(e.country)
      and dol_disclosure_job_order.employer_phone IS  e.phone
      and dol_disclosure_job_order.employer_record_id is null;"""
    session.exec(text(sql_query))

    # Lastly, update the last_seen value for employers if needed.
    sql_query = """update employer_record
    set last_seen = d.last_seen
    from (select employer_record_id, max(last_seen) as last_seen from dol_disclosure_job_order group by employer_record_id) as d
    where d.employer_record_id = employer_record.id
    and d.last_seen > employer_record.last_seen;"""
    session.exec(text(sql_query))


def update_employer_records_from_seasonal_jobs(session: Session):
    # First, pull unique sets of employer name, trade name, etc.
    # from the disclosure records and assign those to new employer
    # records if there aren't already matching employer records.
    sql_query = """
          INSERT INTO employer_record(first_seen, last_seen, source, name, trade_name_dba, city, state, country, phone)
            select min(s.first_seen),
                   max(s.last_seen),
                   max(s.source),
                   max(s.employer_name)    as employer_name,
                   max(s.trade_name_dba)   as trade_name_dba,
                   max(s.employer_city)    as employer_city,
                   max(s.employer_state)   as employer_state,
                   max(s.employer_country) as employer_country,
                   s.employer_phone        as employer_phone
            from seasonal_jobs_job_order s
                     join (SELECT DISTINCT lower(seasonal.employer_name)    as employer_name,
                                           lower(seasonal.trade_name_dba)   as trade_name_dba,
                                           lower(seasonal.employer_city)    as employer_city,
                                           lower(seasonal.employer_state)   as employer_state,
                                           lower(seasonal.employer_country) as employer_country,
                                           seasonal.employer_phone          as employer_phone
                           FROM seasonal_jobs_job_order seasonal
                                    left outer join employer_record e
                                                    on lower(seasonal.employer_name) IS lower(e.name) and
                                                       lower(seasonal.trade_name_dba) IS lower(e.trade_name_dba) and
                                                       lower(seasonal.employer_city) IS lower(e.city) and
                                                       lower(seasonal.employer_state) IS lower(e.state) and
                                                       lower(seasonal.employer_country) IS lower(e.country) and
                                                       seasonal.employer_phone IS e.phone
                           where seasonal.scraped and e.name is null and seasonal.employer_record_id is null) u
                          on lower(s.employer_name) IS u.employer_name and lower(s.trade_name_dba) IS u.trade_name_dba and
                             lower(s.employer_city) IS u.employer_city and lower(s.employer_state) IS u.employer_state and
                             lower(s.employer_country) IS u.employer_country and s.employer_phone IS u.employer_phone
            group by u.employer_name, u.trade_name_dba, u.employer_city, u.employer_state, u.employer_country, u.employer_phone;
            """

    session.exec(text(sql_query))

    # Next, update the linked employer_record_ids.
    sql_query = """update seasonal_jobs_job_order
        set employer_record_id = e.id
        from (select id, name, trade_name_dba, city, state, country, phone from employer_record) as e
        where lower(seasonal_jobs_job_order.employer_name) IS lower(e.name)
          and lower(seasonal_jobs_job_order.trade_name_dba) IS lower(e.trade_name_dba)
          and lower(seasonal_jobs_job_order.employer_city) IS lower(e.city)
          and lower(seasonal_jobs_job_order.employer_state) IS lower(e.state)
          and lower(seasonal_jobs_job_order.employer_country) IS lower(e.country)
          and seasonal_jobs_job_order.employer_phone IS e.phone
          and seasonal_jobs_job_order.employer_record_id is null;"""
    session.exec(text(sql_query))

    # Lastly, update the last_seen value for employers if needed.
    sql_query = """update employer_record
    set last_seen = d.last_seen
    from (select employer_record_id, max(last_seen) as last_seen from seasonal_jobs_job_order group by employer_record_id) as d
    where d.employer_record_id = employer_record.id
    and d.last_seen > employer_record.last_seen;"""
    session.exec(text(sql_query))


def update_employer_records():
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


if __name__ == "__main__":
    update_employer_records()
