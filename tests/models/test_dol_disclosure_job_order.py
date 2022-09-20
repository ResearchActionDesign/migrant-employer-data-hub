from unittest import TestCase

from sqlmodel import Session

from db import get_mock_engine
from models.dol_disclosure_job_order import DolDisclosureJobOrder


class TestDolDisclosureJobOrder(TestCase):
    def setUp(self):
        engine = get_mock_engine()
        self.session = Session(engine)

    def test_cleans_trade_name(self):
        test_listing = DolDisclosureJobOrder(trade_name_dba='DBA test business ')
        test_listing.clean()
        self.assertEqual(test_listing.trade_name_dba, "test business")

    def test_cleans_field(self):
        test_listing = DolDisclosureJobOrder(
            employer_name=" Test business ",
            trade_name_dba="Test trade name",
            employer_city="Test city",
            employer_state="California",
            employer_postal_code="Test zip ",
        )
        test_listing.clean()
        self.assertEqual(test_listing.employer_name, 'Test business')
        self.assertEqual(test_listing.trade_name_dba, "Test trade name")
        self.assertEqual(test_listing.employer_city, "Test city")
        self.assertEqual(test_listing.employer_state,"CA")
        self.assertEqual(test_listing.employer_postal_code, "Test zip")





