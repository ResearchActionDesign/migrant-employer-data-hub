import datetime
from unittest import TestCase

import pytest
from sqlmodel import Session, select

# from actions import normalize_addresses
from db import drop_all_models, get_mock_engine
from models.address_record import AddressRecord


@pytest.mark.skip
class TestNormalizeAddresses(TestCase):
    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        self.capsys = capsys

    @pytest.fixture(autouse=True)
    def monkeypatch(self, monkeypatch):
        self.monkeypatch = monkeypatch

    def setUp(self):
        engine = get_mock_engine()
        self.session = Session(engine)
        self.monkeypatch.setattr(normalize_addresses, 'get_engine', get_mock_engine)

    def tearDown(self):
        drop_all_models()
        self.session.close()

    @pytest.mark.skip
    def test_normalize_address(self):
        test_address_1 = AddressRecord(
            address_1='123 Main St',
            city='Durham'
        )
        test_address_2 = AddressRecord(
            address_1='123 Main Street',
            address_2='Suite 123',
            city='Durham'
        )
        test_address_3 = AddressRecord(
            address_1='123B W Johnson St'
        )
        self.session.add(test_address_1)
        self.session.add(test_address_2)
        self.session.add(test_address_3)

        self.session.commit()

        normalize_addresses.normalize_addresses()

        self.session.refresh(test_address_1)
        self.assertEqual('123 main street', test_address_1.normalized_address)