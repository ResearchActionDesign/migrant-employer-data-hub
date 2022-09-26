from postal import expand
from sqlalchemy import null
from sqlmodel import Session, select

from db import get_engine
from models.address_record import AddressRecord
from settings import ROWS_BEFORE_COMMIT


def normalize_addresses(batch_num=-1):
    """
    Parse through all addresses in the address table and add the normalized_address field if it is missing.

    Uses pypostal.
    :return:
    """

    with Session(get_engine()) as session:
        query = select(AddressRecord).where(AddressRecord.normalized_address == null())
        if batch_num > 0:
            query = query.limit(batch_num)

        addresses_to_normalize = session.exec(query)
        i = 0

        for address in addresses_to_normalize:
            address.normalized_address = expand.expand_address(
                f'{address.address_1} {address.address_2 if address.address_2 else ""}'.strip(),
                languages=["en"],
                address_components=expand.ADDRESS_CATEGORY
                | expand.ADDRESS_ENTRANCE
                | expand.ADDRESS_HOUSE_NUMBER
                | expand.ADDRESS_LEVEL
                | expand.ADDRESS_NAME
                | expand.ADDRESS_NEAR
                | expand.ADDRESS_PO_BOX
                | expand.ADDRESS_STAIRCASE
                | expand.ADDRESS_STREET
                | expand.ADDRESS_UNIT,
            )
            session.add(address)
            i += 1

            if i % ROWS_BEFORE_COMMIT == 0:
                print(f"Normalized {i} addresses")
                session.commit()

        session.commit()


if __name__ == "__main__":
    normalize_addresses(500)
