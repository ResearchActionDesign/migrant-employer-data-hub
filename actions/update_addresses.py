from datetime import datetime
from typing import List

from sqlalchemy import null
from sqlmodel import Session, select

from db import get_engine
from models.address_record import AddressRecord
from models.base import DoLDataSource
from models.dol_disclosure_job_order import DolDisclosureJobOrder
from models.dol_disclosure_job_order_address_record_link import (
    DolDisclosureJobOrderAddressRecordLink,
)
from models.employer_record import EmployerRecord
from models.employer_record_address_link import AddressType, EmployerRecordAddressLink


def title_case_or_none(obj: str | None) -> str | object:
    if obj is not None:
        return str(obj).title()
    return null()


def link_address_to_employer(
    session: Session,
    address: AddressRecord,
    employer: EmployerRecord,
    address_type: AddressType,
    first_seen: datetime,
    last_seen: datetime,
    source: DoLDataSource,
):
    """
    Link a single address to a single employer record, or update existing link if it exists.
    :param session
    :param address:
    :param employer:
    :param address_type: type of address for the linkage
    :param first_seen
    :param last_seen
    :param source
    :return:
    """

    # First, check for existing links.
    existing_links = session.exec(
        select(EmployerRecordAddressLink)
        .where(EmployerRecordAddressLink.employer_record == employer)
        .where(EmployerRecordAddressLink.address_record == address)
        .where(EmployerRecordAddressLink.address_type == address_type)
    ).all()
    if len(existing_links) > 1:
        print(
            f"Error! Multiple address - employer record links found for {address_type} {address} <-> {employer.name}"
        )
        return

    save_record = False
    if len(existing_links) == 1:
        if existing_links[0].first_seen > first_seen:
            existing_links[0].first_seen = first_seen
            save_record = True
        if existing_links[0].last_seen < last_seen:
            existing_links[0].last_seen = last_seen
            save_record = True

        if save_record:
            session.add(existing_links[0])

    else:
        session.add(
            EmployerRecordAddressLink(
                employer_record=employer,
                address_record=address,
                address_type=address_type,
                first_seen=first_seen,
                last_seen=last_seen,
                source=source,
            )
        )
    return


def check_for_matching_addresses(
    search_address: AddressRecord,
    session: Session,
    local_addresses=None,
) -> List[AddressRecord]:
    """
    Checks the DB for addresses matching a given address.
    :param address_attributes:
    :param local_addresses: Addresses which haven't yet been committed to the DB
    :param session:
    :return:
    """

    if local_addresses is None:
        local_addresses = []
    if len(local_addresses) > 0:
        # First, search through local addresses.
        string_match = str(search_address)
        matching_local_addresses = [
            a for a in local_addresses if str(a) == string_match
        ]
        if len(matching_local_addresses) > 0:
            return matching_local_addresses

    statement = select(AddressRecord).where(
        AddressRecord.address_1 == search_address.address_1,
        AddressRecord.address_2 == search_address.address_2,
        AddressRecord.city == search_address.city,
        AddressRecord.state == search_address.state,
        AddressRecord.postal_code == search_address.postal_code,
        AddressRecord.country == search_address.country,
    )
    return session.exec(statement).all()


def process_job_order(
    job_order: DolDisclosureJobOrder,
    session: Session,
    local_addresses=None,
) -> (DolDisclosureJobOrder, List[AddressRecord]):
    """
    Process addresses from a single job order.

    :param job_order:
    :param session:
    :return:
    """
    # First, check for matching office addresses.
    if local_addresses is None:
        local_addresses = []
    office_address = AddressRecord(
        address_1=job_order.employer_address_1,
        address_2=job_order.employer_address_2,
        city=job_order.employer_city,
        state=job_order.employer_state,
        postal_code=job_order.employer_postal_code,
        country=job_order.employer_country,
    ).clean()

    if not office_address.is_null():
        matching_addresses = check_for_matching_addresses(
            office_address, session, local_addresses=local_addresses
        )
        if len(matching_addresses) == 0:
            # Create a new address record if none exists.
            matching_addresses = [
                office_address,
            ]
            session.add(office_address)
            local_addresses.append(office_address)

        for address in matching_addresses:
            link_address_to_employer(
                session,
                address,
                job_order.employer_record,
                AddressType.office,
                job_order.first_seen,
                job_order.last_seen,
                job_order.source,
            )
            job_order.address_records.append(address)

    # Then do the same for matching jobsite addresses.
    # First create a new address and check if it is null and/or if it is the same as the previously created address.
    jobsite_address = AddressRecord(
        address_1=job_order.worksite_address,
        city=job_order.worksite_city,
        state=job_order.worksite_state,
        postal_code=job_order.worksite_postal_code,
    ).clean()

    if jobsite_address.is_null():
        return (job_order, local_addresses)

    matching_addresses = check_for_matching_addresses(
        jobsite_address, session, local_addresses=local_addresses
    )
    if len(matching_addresses) == 0:
        # Create a new address record if none exists.
        matching_addresses = [
            jobsite_address,
        ]
        session.add(jobsite_address)
        local_addresses.append(jobsite_address)

    for address in matching_addresses:
        link_address_to_employer(
            session,
            address,
            job_order.employer_record,
            AddressType.jobsite,
            job_order.first_seen,
            job_order.last_seen,
            job_order.source,
        )
        job_order.address_records.append(address)
    return (job_order, local_addresses)


def update_addresses(max_records: int = -1):
    """
    Scan through DoL Disclosure table and create new records for each unique address, linked to the employer record.

    :return:
    """

    engine = get_engine()
    session = Session(engine)

    # Get DoL disclosure table records which have not been linked to addresses yet.
    statement = (
        select(DolDisclosureJobOrder)
        .join(DolDisclosureJobOrderAddressRecordLink, isouter=True)
        .where(DolDisclosureJobOrderAddressRecordLink.address_record_id == null())
        .where(DolDisclosureJobOrder.employer_record != null())
    )

    if max_records > 0:
        statement = statement.limit(max_records)

    job_orders_to_process = session.exec(statement)

    local_addresses = []
    i = 0
    for job_order in job_orders_to_process:
        job_order, local_addresses = process_job_order(
            job_order, session, local_addresses=local_addresses
        )
        session.add(job_order)
        i += 1
        if i % 250 == 0:
            print(f"Processed {i} job orders for addresses")
            session.commit()

            if len(local_addresses) > 2500:
                # In-memory comparisons seem to be a bear after a certain amount of records get in this array.
                local_addresses = []

    session.commit()
    session.close()


if __name__ == "__main__":
    update_addresses()
