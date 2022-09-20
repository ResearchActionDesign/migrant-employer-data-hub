from unittest import TestCase

from sqlmodel import Session

from db import get_mock_engine
from models.address_record import AddressRecord


class TestAddressRecord(TestCase):
    def setUp(self):
        engine = get_mock_engine()
        self.session = Session(engine)

    def test_to_string(self):
        test_address = AddressRecord(
            address_1='Address line 1',
            address_2='Address line 2',
            city='City',
            state='NC',
            postal_code='27701',
            country='USA'
        )
        self.assertEqual(str(test_address), "Address line 1 Address line 2, City, NC 27701 USA")

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




