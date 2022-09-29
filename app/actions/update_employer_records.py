from sqlalchemy import text
from sqlmodel import Session

from app.db import get_engine
from app.settings import DB_ENGINE

# flake8: noqa


IS_ = "IS" if DB_ENGINE == "sqlite" else "IS NOT DISTINCT FROM"


def slugify(fieldname: str) -> str:
    """
    Generate SQL query for slugify depending on DB_ENGINE
    :param fieldname:
    :return:
    """
    if DB_ENGINE == "postgres":
        return f"slugify({fieldname})"

    return f"lower({fieldname})"


def get_insert_query(records_table: str) -> str:
    """
    Generates a templated insert query to insert unique records into the employer_record table from records_table.

    records_table must have the following fields:
        * first_seen
        * last_seen
        * source
        * employer_name
        * trade_name_dba
        * employer_city
        * employer_state
        * employer_country
        * employer_phone
        * employer_record_id
    :param records_table:
    :return:
    """
    return f"""
            INSERT INTO employer_record(first_seen, last_seen, source, name, trade_name_dba, city, state, country, phone, slug, trade_name_slug)
        select min(d.first_seen),
               max(d.last_seen),
               max(d.source),
               max(d.employer_name)    as employer_name,
               max(d.trade_name_dba)   as trade_name_dba,
               max(d.employer_city)    as employer_city,
               max(d.employer_state)   as employer_state,
               max(d.employer_country) as employer_country,
               max(d.employer_phone)        as employer_phone,
               max({slugify('d.employer_name')}) as slug,
               max({slugify('d.trade_name_dba')}) as trade_name_slug
        from {records_table} d
                 join (SELECT DISTINCT {slugify('dol.employer_name')} as slug,
                                       {slugify('dol.trade_name_dba')} as trade_name_slug,
                                       lower(dol.employer_city)    as employer_city,
                                       lower(dol.employer_state)   as employer_state,
                                       lower(dol.employer_country) as employer_country,
                                       dol.employer_phone          as employer_phone
                       FROM {records_table} dol
                                left outer join employer_record e
                                                on {slugify('dol.employer_name')} {IS_} e.slug and
                                                   {slugify('dol.trade_name_dba')} {IS_} e.trade_name_slug and
                                                   lower(dol.employer_city) {IS_} lower(e.city) and
                                                   lower(dol.employer_state) {IS_} lower(e.state) and
                                                   lower(dol.employer_country) {IS_} lower(e.country) and
                                                   dol.employer_phone {IS_} e.phone
                       where e.slug is null and dol.employer_record_id is null) u
                      on {slugify('d.employer_name')} {IS_} u.slug and
                         {slugify('d.trade_name_dba')} {IS_} u.trade_name_slug and
                         lower(d.employer_city) {IS_} u.employer_city and
                         lower(d.employer_state) {IS_} u.employer_state and
                         lower(d.employer_country) {IS_} u.employer_country and
                         d.employer_phone {IS_} u.employer_phone

        group by u.slug, u.trade_name_slug, u.employer_city, u.employer_state, u.employer_country, u.employer_phone;
            """


def get_update_employer_record_id_query(records_table: str) -> str:
    """
    Generates a templated update query to add the employer_record_id field to 'records_table' for matching
    records in the employer_record table.

    records_table must have the following fields:
        * first_seen
        * last_seen
        * source
        * employer_name
        * trade_name_dba
        * employer_city
        * employer_state
        * employer_country
        * employer_phone
        * employer_record_id
    :param records_table:
    :return:
    """
    return f"""update {records_table}
        set employer_record_id = (
        select id from employer_record e
        where {slugify(records_table + '.employer_name')} {IS_} lower(e.slug)
          and {slugify(records_table + '.trade_name_dba')} {IS_} lower(e.trade_name_slug)
          and lower({records_table}.employer_city) {IS_} lower(e.city)
          and lower({records_table}.employer_state) {IS_} lower(e.state)
          and lower({records_table}.employer_country) {IS_} lower(e.country)
          and {records_table}.employer_phone {IS_} e.phone)
          where {records_table}.employer_record_id is null;"""


def get_update_last_seen_query(records_table: str) -> str:
    """
    Generates a templated update query to update the last_seen field in the employer_record
     table according to matching records in 'records_table'.

    records_table must have the following fields:
        * first_seen
        * last_seen
        * employer_record_id
    :param records_table:
    :return:
    """

    return f"""update employer_record
        set last_seen = (select max(last_seen)
        from {records_table} where employer_record_id = employer_record.id group by employer_record_id)
        where exists (select last_seen from {records_table} where employer_record_id = employer_record.id and last_seen > employer_record.last_seen);"""


def get_update_first_seen_query(records_table: str) -> str:
    """
    Generates a templated update query to update the last_seen field in the employer_record
     table according to matching records in 'records_table'.

    records_table must have the following fields:
        * first_seen
        * last_seen
        * employer_record_id
    :param records_table:
    :return:
    """

    return f"""update employer_record
        set first_seen = (select min(first_seen)
        from {records_table} where employer_record_id = employer_record.id group by employer_record_id)
        where exists (select last_seen from {records_table} where employer_record_id = employer_record.id and first_seen < employer_record.first_seen);"""


def update_employer_records_from_raw_records_table(
    session: Session, records_table: str
) -> None:
    # First, pull unique sets of employer name, trade name, etc.
    # from the disclosure records and assign those to new employer
    # records if there aren't already matching employer records.
    session.exec(text(get_insert_query(records_table)))

    # Next, update the linked employer_record_ids.
    session.exec(text(get_update_employer_record_id_query(records_table)))

    # Lastly, update the last_seen value for employers if needed.
    session.exec(text(get_update_last_seen_query(records_table)))
    session.exec(text(get_update_first_seen_query(records_table)))
    print(f"Updated employer records from {records_table} table")


def update_employer_records() -> bool:
    """
    Find new unique employer name/city/state/phone combos and add then to the employer records list.
    """

    records_tables = ["dol_disclosure_job_order", "seasonal_jobs_job_order"]

    # Parse through DoL disclosure data table and add any new employers there to the employer records table
    session = Session(get_engine())
    for r in records_tables:
        update_employer_records_from_raw_records_table(session, r)
        session.commit()

    session.close()
    return True


if __name__ == "__main__":
    update_employer_records()
