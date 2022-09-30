from app.models.address_record import AddressRecord
from app.models.dol_disclosure_job_order import DolDisclosureJobOrder  # noqa
from app.models.employer_record_address_link import EmployerRecordAddressLink  # noqa
from app.tests.base_test_case import BaseTestCase


class TestAddressRecord(BaseTestCase):
    use_session = False

    def test_to_string(self):
        test_address = AddressRecord(
            address_1='Address line 1',
            address_2='Address line 2',
            city='City',
            state='NC',
            postal_code='27701',
            country='USA'
        )
        self.assertEqual("Address line 1 Address line 2, City, NC 27701 USA", str(test_address))
        test_address = AddressRecord(
            address_1='Address line 1',
            address_2='Address line 2',
            city=None,
            state=None,
            postal_code=None,
            country='USA'
        )
        self.assertEqual("Address line 1 Address line 2 USA", str(test_address))
        test_address = AddressRecord(
            address_1='Address line 1',
            address_2=None,
            city='Boulder',
            state='CO',
            postal_code='12345',
            country='USA'
        )
        self.assertEqual("Address line 1, Boulder, CO 12345 USA", str(test_address))

    def test_geocode_hash(self):
        test_address = AddressRecord(
            address_1='Address line 1',
            address_2='Address line 2',
            city='City',
            state='NC',
            postal_code='27701',
            country='USA'
        )
        test_address_2 = AddressRecord(
            address_1='Address line 1 - v2',
            address_2='Address line 2',
            city='City',
            state='NC',
            postal_code='27701',
            country='USA'
        )
        self.assertNotEqual(test_address.get_geocode_hash(), test_address_2.get_geocode_hash())

    def test_clean_method(self):
        test_address = AddressRecord(
            address_1=' Address LINE 1 ',
            address_2=' address line 2  ',
            city=None,
            state='north carolina',
            postal_code='27701 ',
            country=None
        )
        test_address.clean()
        self.assertEqual(test_address.address_1,'Address Line 1')
        self.assertEqual(test_address.address_2,'Address Line 2')
        self.assertIsNone(test_address.city)
        self.assertEqual(test_address.state,'NC')
        self.assertEqual(test_address.postal_code,'27701')
        self.assertEqual(test_address.country,'UNITED STATES OF AMERICA')

    def test_not_null_method(self):
        test_address = AddressRecord(
            address_1=' Address LINE 1 ',
            address_2=' address line 2  ',
            city=None,
            state='north carolina',
            postal_code='27701 ',
            country=None
        )
        self.assertFalse(test_address.is_null())
        test_address = AddressRecord(
        )
        self.assertTrue(test_address.is_null())




