from app.models.dol_disclosure_job_order import DolDisclosureJobOrder
from app.tests.base_test_case import BaseTestCase


class TestDolDisclosureJobOrder(BaseTestCase):
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
            employer_phone='1-919-222+2222'
        )
        test_listing.clean()
        self.assertEqual(test_listing.employer_name, 'Test business')
        self.assertEqual(test_listing.trade_name_dba, "Test trade name")
        self.assertEqual(test_listing.employer_city, "Test city")
        self.assertEqual(test_listing.employer_state,"CA")
        self.assertEqual(test_listing.employer_postal_code, "Test zip")
        self.assertEqual(test_listing.employer_phone, '19192222222')

