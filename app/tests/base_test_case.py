from typing import Union
from unittest import TestCase

import pytest
from sqlmodel import Session

from app.db import drop_all_models, get_mock_engine


class BaseTestCase(TestCase):
    session: Union[Session, None] = None
    use_session: bool = True

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        self.capsys = capsys

    @pytest.fixture(autouse=True)
    def monkeypatch(self, monkeypatch):
        self.monkeypatch = monkeypatch

    def setUp(self):
        if self.use_session:
            engine = get_mock_engine()
            self.session = Session(engine)

    def tearDown(self):
        if self.use_session:
            drop_all_models()
            self.session.close()