from datetime import datetime
from typing import Dict, List, Tuple, Union

from sqlalchemy import null
from sqlmodel import Session, select

from app.db import get_engine
from app.models.address_record import AddressRecord, normalize_address
from app.models.base import DoLDataSource
from app.models.dol_disclosure_job_order import DolDisclosureJobOrder
from app.models.dol_disclosure_job_order_address_record_link import (
    DolDisclosureJobOrderAddressRecordLink,
)
from app.models.employer_record import EmployerRecord
from app.models.employer_record_address_link import (
    AddressType,
    EmployerRecordAddressLink,
)
from app.settings import ROWS_BEFORE_COMMIT

address_normalize_session = Session(get_engine())


def link_address_to_employer(
    session: Session,
    address_id: int,
    employer: EmployerRecord,
    address_type: AddressType,
    first_seen: Union[datetime, None],
    last_seen: Union[datetime, None],
    source: DoLDataSource,
) -> None:
    """
    Link a single address to a single employer record, or update existing link if it exists.
    :param session
    :param address_id:
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
        .where(EmployerRecordAddressLink.address_record_id == address_id)
        .where(EmployerRecordAddressLink.address_type == address_type)
    ).all()
    if len(existing_links) > 1:
        print(
            f"Error! Multiple address - employer record links found for {address_type} {address_id} <-> {employer.name}"
        )
        return

    save_record = False
    if len(existing_links) == 1:
        if not existing_links[0].first_seen or (
            first_seen and existing_links[0].first_seen > first_seen
        ):
            existing_links[0].first_seen = first_seen
            save_record = True
        if not existing_links[0].last_seen or (
            last_seen and existing_links[0].last_seen < last_seen
        ):
            existing_links[0].last_seen = last_seen
            save_record = True

        if save_record:
            session.add(existing_links[0])

    else:
        session.add(
            EmployerRecordAddressLink(
                employer_record=employer,
                address_record_id=address_id,
                address_type=address_type,
                first_seen=first_seen,
                last_seen=last_seen,
                source=source,
            )
        )


def check_for_matching_addresses(
    address: AddressRecord,
    session: Session,
    local_addresses: Union[Dict[str, int], None] = None,
) -> List[int]:
    """
    Checks the DB for addresses matching a given address.
    :param address_attributes:
    :param local_addresses: Addresses which haven't yet been committed to the DB
    :param session:
    :return:
    """

    if local_addresses is None:
        local_addresses = {}
    if address.normalized_address is None:
        return []

    if len(local_addresses) > 0:
        # First, search through local addresses.
        matching_local_addresses = local_addresses.get(address.normalized_address)
        if matching_local_addresses:
            return [
                matching_local_addresses,
            ]

    statement = select(AddressRecord.id).where(
        AddressRecord.normalized_address == address.normalized_address
    )
    return session.exec(statement).all()


def process_job_order(
    job_order: DolDisclosureJobOrder,
    session: Session,
    local_addresses: Union[Dict[str, AddressRecord], None] = None,
) -> Tuple[DolDisclosureJobOrder, List[AddressRecord]]:
    """
    Process addresses from a single job order.

    :param job_order:
    :param session:
    :return:
    """
    # First, check for matching office addresses.
    if local_addresses is None:
        local_addresses = {}
    office_address = AddressRecord(
        address_1=job_order.employer_address_1,
        address_2=job_order.employer_address_2,
        city=job_order.employer_city,
        state=job_order.employer_state,
        postal_code=job_order.employer_postal_code,
        country=job_order.employer_country,
    ).clean()
    office_address.normalized_address = normalize_address(
        str(office_address), address_normalize_session
    )

    if not office_address.is_null():
        matching_addresses = check_for_matching_addresses(
            office_address, session, local_addresses=local_addresses
        )
        if len(matching_addresses) == 0:
            # Create a new address record if none exists.
            session.add(office_address)
            session.commit()
            session.refresh(office_address)
            local_addresses[office_address.normalized_address] = office_address.id
            matching_addresses = [
                office_address.id,
            ]

        if len(matching_addresses) > 1:
            print(
                f"Error -- more than one address found matching {office_address.normalized_address}"
            )

        link_address_to_employer(
            session,
            matching_addresses[0],
            job_order.employer_record,
            AddressType.office,
            job_order.first_seen,
            job_order.last_seen,
            job_order.source,
        )
        session.add(
            DolDisclosureJobOrderAddressRecordLink(
                dol_disclosure_job_order_id=job_order.id,
                address_record_id=matching_addresses[0],
            )
        )
        office_address_id = matching_addresses[0]

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
    jobsite_address.normalized_address = normalize_address(
        str(jobsite_address), address_normalize_session
    )

    matching_addresses = check_for_matching_addresses(
        jobsite_address, session, local_addresses=local_addresses
    )
    if len(matching_addresses) == 0:
        # Create a new address record if none exists.
        session.add(jobsite_address)
        session.commit()
        session.refresh(jobsite_address)
        local_addresses[jobsite_address.normalized_address] = jobsite_address.id
        matching_addresses = [
            jobsite_address.id,
        ]

    if len(matching_addresses) > 1:
        print(
            f"Error -- more than one address found matching {jobsite_address.normalized_address}"
        )

    link_address_to_employer(
        session,
        matching_addresses[0],
        job_order.employer_record,
        AddressType.jobsite,
        job_order.first_seen,
        job_order.last_seen,
        job_order.source,
    )
    if matching_addresses[0] != office_address_id:
        session.add(
            DolDisclosureJobOrderAddressRecordLink(
                dol_disclosure_job_order_id=job_order.id,
                address_record_id=matching_addresses[0],
            )
        )

    session.commit()

    return (job_order, local_addresses)


def update_addresses(max_records: int = -1) -> None:
    """
    Scan through DoL Disclosure table and create new records for each unique address, linked to the employer record.

    :return:
    """

    engine = get_engine()
    session = Session(engine, autoflush=False)

    # Get DoL disclosure table records which have not been linked to addresses yet.
    statement = (
        select(DolDisclosureJobOrder)
        .join(DolDisclosureJobOrderAddressRecordLink, isouter=True)
        .where(DolDisclosureJobOrderAddressRecordLink.address_record_id == null())
    )

    if max_records > 0:
        statement = statement.limit(max_records)

    job_orders_to_process = session.exec(statement)

    local_addresses: Dict[str, int] = {}
    i = 0
    for job_order in job_orders_to_process:
        job_order, local_addresses = process_job_order(
            job_order, session, local_addresses=local_addresses
        )
        i += 1
        if i % ROWS_BEFORE_COMMIT == 0:
            print(f"Processed {i} job orders for addresses")
            session.commit()

    session.commit()
    session.close()


if __name__ == "__main__":
    update_addresses(10)
