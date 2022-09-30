import datetime

from sqlmodel import Session, select

from app.actions import update_addresses, update_employer_records
from app.db import drop_all_models, get_mock_engine
from app.models.address_record import AddressRecord
from app.models.base import DoLDataSource
from app.models.dol_disclosure_job_order import DolDisclosureJobOrder
from app.models.employer_record import EmployerRecord
from app.models.employer_record_address_link import AddressType
from app.tests.base_test_case import BaseTestCase


class TestUpdateAddresses(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.monkeypatch.setattr(update_employer_records, 'get_engine', get_mock_engine)
        self.monkeypatch.setattr(update_addresses, 'get_engine', get_mock_engine)

    def test_generate_address_records(self):
        test_listing = DolDisclosureJobOrder(
            employer_name="Test business",
            trade_name_dba="Test trade name",
            employer_address_1="Address 1",
            employer_city="Test city",
            employer_state="California",
            employer_country="UNITED STATES OF AMERICA",
            employer_postal_code="Test zip ",
            first_seen=datetime.datetime(1999,1,1),
            last_seen=datetime.datetime(2000,1,1),
            worksite_address="Test address 123",
            worksite_city="Test city #2",
            worksite_state="NC",
            worksite_postal_code="12345"
        ).clean()
        test_listing_2 = DolDisclosureJobOrder(
            employer_name="Test business",
            trade_name_dba="TEST TRADE NAME",
            employer_address_1="Address 1",
            employer_address_2="Address 2",
            employer_city="TEST CITY",
            employer_state="California",
            employer_postal_code="Test zip 2",
            employer_country="UNITED STATES OF AMERICA",
            first_seen=datetime.datetime(1999, 1, 1),
            last_seen=datetime.datetime(2002, 1, 1)
        ).clean()
        test_listing_3 = DolDisclosureJobOrder(
            employer_name="test business #2",
            trade_name_dba=None,
            employer_address_1="Address 1",
            employer_address_2="Address 2",
            employer_city="Test city",
            employer_state="California",
            employer_postal_code="Test zip 2",
            employer_country="UNITED STATES OF AMERICA",
            first_seen=datetime.datetime(2003, 1, 1),
            last_seen=datetime.datetime(2004, 1, 1)
        ).clean()
        self.session.add(test_listing)
        self.session.add(test_listing_2)
        self.session.add(test_listing_3)

        self.session.commit()

        # First, generate employer records so we have them.
        update_employer_records.update_employer_records()
        update_addresses.update_addresses()

        # Sanity check that initial job records and employer records got created.
        self.assertEqual(3, len(self.session.exec(select(DolDisclosureJobOrder)).all()))
        self.assertEqual(2, len(self.session.exec(select(EmployerRecord)).all()))

        # The above 3 Dol disclosure rows should collapse to 3 distinct addresses.
        all_addresses = self.session.exec(select(AddressRecord)).all()
        self.assertEqual(3, len(all_addresses))

        # Check that links are properly being created from DoL Disclosure records to addresses.
        self.session.refresh(test_listing)
        self.session.refresh(test_listing_2)
        self.session.refresh(test_listing_3)

        self.assertEqual(2, len(test_listing.address_records))
        self.assertEqual(1, len(test_listing_2.address_records))
        self.assertEqual(1, len(test_listing_3.address_records))

        self.assertEqual("Address 1, Test City, CA Test zip UNITED STATES OF AMERICA", str(test_listing.address_records[0]))
        self.assertEqual("Test Address 123, Test City #2, NC 12345 UNITED STATES OF AMERICA", str(test_listing.address_records[1]))

        # Check that links are being created from Employer records to addresses.
        employer_1 = self.session.exec(select(EmployerRecord).where(EmployerRecord.name == 'Test business')).first()
        self.assertEqual(3, len(employer_1.address_record_links))

        employer_2 = self.session.exec(select(EmployerRecord).where(EmployerRecord.name == 'test business #2')).first()
        self.assertEqual(1, len(employer_2.address_record_links))
        self.assertEqual(datetime.datetime(2003, 1, 1), employer_2.address_record_links[0].first_seen)
        self.assertEqual(datetime.datetime(2004, 1, 1), employer_2.address_record_links[0].last_seen)
        self.assertEqual(DoLDataSource.dol_disclosure, employer_2.address_record_links[0].source)
        self.assertEqual(AddressType.office, employer_2.address_record_links[0].address_type)

        # Check that it updates the last seen / first seen when an address re-appears.
        test_listing_4 = DolDisclosureJobOrder(
            employer_name="test business #2",
            trade_name_dba=None,
            employer_address_1="Address 1",
            employer_address_2="Address 2",
            employer_city="Test city",
            employer_state="California",
            employer_postal_code="Test zip 2",
            employer_country="UNITED STATES OF AMERICA",
            first_seen=datetime.datetime(2003, 1, 1),
            last_seen=datetime.datetime(2007, 1, 1)
        ).clean()
        self.session.add(test_listing_4)
        self.session.commit()

        update_employer_records.update_employer_records()
        update_addresses.update_addresses()

        all_addresses = self.session.exec(select(AddressRecord)).all()
        self.assertEqual(3, len(all_addresses))
        self.session.refresh(employer_2)
        self.assertEqual(datetime.datetime(2007, 1, 1), employer_2.address_record_links[0].last_seen)
