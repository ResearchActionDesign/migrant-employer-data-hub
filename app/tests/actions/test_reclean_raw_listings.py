from unittest import TestCase

import pytest
from sqlmodel import Session

from app.actions import reclean_raw_listings
from app.db import drop_all_models, get_mock_engine
from app.models.dol_disclosure_job_order import DolDisclosureJobOrder
from app.models.seasonal_jobs_job_order import SeasonalJobsJobOrder
from app.tests.base_test_case import BaseTestCase


class TestRecleanRawListings(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.monkeypatch.setattr(reclean_raw_listings, 'get_engine', get_mock_engine)

    def test_recleans_fields(self):
        test_listing = DolDisclosureJobOrder(
            employer_name=" Test business ",
            trade_name_dba="Test trade name",
            employer_city="Test city",
            employer_state="California",
            employer_postal_code="Test zip ",
            employer_phone='1-919-222+2222'
        )
        self.session.add(test_listing)
        test_listing_2 = SeasonalJobsJobOrder(title='Foobar', scraped_data={
            "employer_business_name": " Test business ",
            "employer_trade_name": "Test trade name",
            "employer_city": "Test city",
            "employer_state": "California",
            "employer_zip": "Test zip",
            "employer_phone": "1-919-222A"
        })
        self.session.add(test_listing_2)
        self.session.commit()

        reclean_raw_listings.reclean_raw_listings()

        self.session.refresh(test_listing)
        self.session.refresh(test_listing_2)

        self.assertEqual(test_listing.employer_name, 'Test business')
        self.assertEqual(test_listing.trade_name_dba, "Test trade name")
        self.assertEqual(test_listing.employer_city, "Test city")
        self.assertEqual(test_listing.employer_state,"CA")
        self.assertEqual(test_listing.employer_postal_code, "Test zip")
        self.assertEqual(test_listing.employer_phone, '19192222222')

        self.assertEqual(test_listing_2.employer_name, 'Test business')
        self.assertEqual(test_listing_2.trade_name_dba, "Test trade name")
        self.assertEqual(test_listing_2.employer_city, "Test city")
        self.assertEqual(test_listing_2.employer_state, "CA")
        self.assertEqual(test_listing_2.employer_postal_code, "Test zip")
        self.assertEqual(test_listing_2.employer_phone, "1919222")
