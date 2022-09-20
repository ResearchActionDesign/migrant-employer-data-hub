import time
from io import StringIO
from unittest import TestCase
from unittest.mock import patch

from sqlmodel import Session

from db import get_mock_engine
from models.seasonal_jobs_job_order import SeasonalJobsJobOrder


class TestSeasonalJobsJobOrder(TestCase):
    def setUp(self):
        engine = get_mock_engine()
        self.session = Session(engine)

    def test_cleans_url(self):
        test_listing = SeasonalJobsJobOrder(scraped_data={"apply_url": "N/A"})
        test_listing.clean()
        self.assertEqual(test_listing.scraped_data["apply_url"], "")
        test_listing.scraped_data["apply_url"] = "https://http:www.twc.state.tx.us"
        test_listing.clean()
        self.assertEqual(
            test_listing.scraped_data["apply_url"], "https://www.twc.state.tx.us"
        )
        test_listing.scraped_data["apply_url"] = "https://test.com"
        test_listing.clean()
        self.assertEqual(test_listing.scraped_data["apply_url"], "https://test.com")

    def test_populates_employer_fields(self):
        test_listing = SeasonalJobsJobOrder(scraped_data={
            "employer_business_name": " Test business ",
            "employer_trade_name": "Test trade name",
            "employer_city": "Test city",
            "employer_state": "California",
            "employer_zip": "Test zip",
            "employer_phone": "Test phone"
        })
        test_listing.clean()
        self.assertEqual(test_listing.employer_name, 'Test business')
        self.assertEqual(test_listing.trade_name_dba, "Test trade name")
        self.assertEqual(test_listing.employer_city, "Test city")
        self.assertEqual(test_listing.employer_state,"CA")
        self.assertEqual(test_listing.employer_postal_code, "Test zip")
        self.assertEqual(test_listing.employer_phone, "Test phone")

    def test_handles_null_fields(self):
        test_listing = SeasonalJobsJobOrder(scraped_data={
            "employer_business_name": " Test business ",
            "employer_trade_name": "",
            "employer_city": None,
        })
        test_listing.clean()
        self.assertEqual(test_listing.employer_name, 'Test business')
        self.assertIsNone(test_listing.trade_name_dba)
        self.assertIsNone(test_listing.employer_city)




