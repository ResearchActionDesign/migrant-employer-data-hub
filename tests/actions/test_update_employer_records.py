import datetime
from unittest import TestCase

import pytest
from sqlmodel import Session, select

from actions import update_employer_records
from db import drop_all_models, get_mock_engine
from models.dol_disclosure_job_order import DolDisclosureJobOrder
from models.employer_record import EmployerRecord
from models.seasonal_jobs_job_order import SeasonalJobsJobOrder


class TestUpdateEmployerRecords(TestCase):
    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        self.capsys = capsys

    @pytest.fixture(autouse=True)
    def monkeypatch(self, monkeypatch):
        self.monkeypatch = monkeypatch

    def setUp(self):
        engine = get_mock_engine()
        self.session = Session(engine)
        self.monkeypatch.setattr(update_employer_records, 'get_engine', get_mock_engine)

    def tearDown(self):
        drop_all_models()
        self.session.close()

    def test_generates_employer_records(self):
        test_listing = DolDisclosureJobOrder(
            employer_name="Test business",
            trade_name_dba="Test trade name",
            employer_address_1="This should be ignored",
            employer_city="Test city",
            employer_state="California",
            employer_country="UNITED STATES OF AMERICA",
            employer_postal_code="Test zip ",
            first_seen=datetime.date(1999,1,1),
            last_seen=datetime.date(2000,1,1)
        ).clean()
        test_listing_2 = DolDisclosureJobOrder(
            employer_name="Test business",
            trade_name_dba="TEST TRADE NAME",
            employer_address_2="This is different, but it should be ignored",
            employer_city="TEST CITY",
            employer_state="California",
            employer_postal_code="Test zip ",
            employer_country="UNITED STATES OF AMERICA",
            first_seen=datetime.datetime(1999, 1, 1),
            last_seen=datetime.datetime(2002, 1, 1)
        ).clean()
        test_listing_3 = DolDisclosureJobOrder(
            employer_name="test business #2",
            trade_name_dba=None,
            employer_city="Test city",
            employer_state="California",
            employer_postal_code="Test zip ",
            employer_country="UNITED STATES OF AMERICA",
            first_seen=datetime.datetime(2003, 1, 1),
            last_seen=datetime.datetime(2004, 1, 1)
        ).clean()
        self.session.add(test_listing)
        self.session.add(test_listing_2)
        self.session.add(test_listing_3)

        test_scraper_listing = SeasonalJobsJobOrder(title="Seasonal jobs 1", scraped=True,
                                                    last_seen = datetime.datetime(2010, 1, 1),
                                                    scraped_data={
            "employer_business_name": " Test business ",
            "employer_trade_name": "Test trade name",
            "employer_city": "Test city",
            "employer_state": "California",
            "employer_zip": "Test zip",
        }).clean()
        test_scraper_listing_2 = SeasonalJobsJobOrder(title="Seasonal jobs 2", scraped=True, scraped_data={
            "employer_business_name": " Test business #3 ",
            "employer_trade_name": "Test trade name",
            "employer_city": "Test city",
            "employer_state": "California",
            "employer_zip": "Test zip",
            "employer_phone": "1234578"
        }).clean()
        self.session.add(test_scraper_listing)
        self.session.add(test_scraper_listing_2)
        self.session.commit()

        update_employer_records.update_employer_records()

        # Sanity check that initial job records got created.
        self.assertEqual(3, len(self.session.exec(select(DolDisclosureJobOrder)).all()))
        self.assertEqual(2, len(self.session.exec(select(SeasonalJobsJobOrder)).all()))

        # The above 3 Dol disclosure rows + 2 seasonal jobs rows should collapse to 3 distinct employers.
        all_employers = self.session.exec(select(EmployerRecord)).all()
        self.assertEqual(3, len(all_employers))
        self.assertEqual(set(['Test business', 'test business #2', 'Test business #3']), set([e.name for e in all_employers]))

        test_employer_1 = self.session.exec(select(EmployerRecord).where(EmployerRecord.name=='Test business')).one()
        self.assertEqual(datetime.datetime(1999, 1, 1), test_employer_1.first_seen)
        self.assertEqual(datetime.datetime(2010, 1, 1), test_employer_1.last_seen)

        # Check that employer records are properly linked back.
        self.assertEqual(test_employer_1.id, test_listing.employer_record_id)
        self.assertEqual(test_employer_1.id, test_listing_2.employer_record_id)
        self.assertEqual(test_employer_1.id, test_scraper_listing.employer_record_id)

        self.assertIsNotNone(test_listing_3.employer_record_id)
        self.assertIsNotNone(test_scraper_listing_2.employer_record_id)

        self.assertNotEqual(test_employer_1.id, test_listing_3.employer_record_id)
        self.assertIsNotNone(test_employer_1.id, test_scraper_listing_2.employer_record_id)
