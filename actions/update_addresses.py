from datetime import datetime

from sqlmodel import Session, select

from db import get_engine
from models.address_record import AddressRecord
from models.dol_disclosure_job_order import DolDisclosureJobOrder
from models.dol_disclosure_job_order_address_record_link import (
    DolDisclosureJobOrderAddressRecordLink,
)
from models.employer_record import EmployerRecord
from models.employer_record_address_link import AddressType, EmployerRecordAddressLink


def link_address_to_employer(
    session: Session,
    address: AddressRecord,
    employer: EmployerRecord,
    address_type: AddressType,
    first_seen: datetime,
    last_seen: datetime,
):
    """
    Link a single address to a single employer record, or update existing link if it exists.
    :param session
    :param address:
    :param employer:
    :param address_type: type of address for the linkage
    :param first_seen
    :param last_seen
    :return:
    """

    # First, check for existing links.
    existing_links = session.exec(
        select(EmployerRecordAddressLink)
        .where(employer_record=employer)
        .where(address_record=address)
        .where(address_type=address_type)
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
        if existing_links[0].last_seen < first_seen:
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
            )
        )

    return


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
        .where(DolDisclosureJobOrderAddressRecordLink.address_record_id is None)
        .where(DolDisclosureJobOrder.employer_record is not None)
    )

    if max_records > 0:
        statement = statement.limit(max_records)

    job_orders_to_process = session.exec(statement)

    for job_order in job_orders_to_process:

        # First, check for matching office addresses.
        statement = (
            select(AddressRecord)
            .where(AddressRecord.address_1 == job_order.employer_address_1.title())
            .where(AddressRecord.address_2 == job_order.employer_address_2.title())
            .where(AddressRecord.city == job_order.employer_city.title())
            .where(AddressRecord.state == job_order.employer_state.upper())
            .where(AddressRecord.postal_code == job_order.employer_postal_code)
            .where(AddressRecord.country == job_order.employer_country.upper())
        )

        matching_addresses = session.exec(statement)

        if len(matching_addresses) == 0:
            # Create a new address record if none exists.
            matching_addresses = [
                AddressRecord(
                    address_1=job_order.employer_address_1,
                    address_2=job_order.employer_address_2,
                    city=job_order.employer_city,
                    state=job_order.employer_state,
                    postal_code=job_order.employer_postal_code,
                    country=job_order.employer_country,
                ).clean()
            ]
            session.add(matching_addresses[0])

        for address in matching_addresses:
            link_address_to_employer(
                session,
                address,
                job_order.employer_record,
                AddressType.office,
                job_order.first_seen,
                job_order.last_seen,
            )
            job_order.address_records.append(address)

        # Then do the same for matching jobsite addresses.
        statement = (
            select(AddressRecord)
            .where(AddressRecord.address_1 == job_order.worksite_address.title())
            .where(AddressRecord.city == job_order.worksite_city.title())
            .where(AddressRecord.state == job_order.worksite_state.upper())
            .where(AddressRecord.postal_code == job_order.worksite_postal_code)
        )
        matching_addresses = session.exec(statement)

        if len(matching_addresses) == 0:
            # Create a new address record if none exists.
            matching_addresses = [
                AddressRecord(
                    address_1=job_order.worksite_address,
                    city=job_order.worksite_city,
                    state=job_order.worksite_state,
                    postal_code=job_order.worksite_postal_code,
                ).clean()
            ]
            session.add(matching_addresses[0])

        for address in matching_addresses:
            link_address_to_employer(
                session,
                address,
                job_order.employer_record,
                AddressType.jobsite,
                job_order.first_seen,
                job_order.last_seen,
            )
            job_order.address_records.append(address)

        session.add(job_order)
        session.commit()


if __name__ == "__main__":
    update_addresses(10)
