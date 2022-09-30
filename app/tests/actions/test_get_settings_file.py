import os

from app.actions import dedupe
from app.settings import BASE_DIR
from app.tests.base_test_case import BaseTestCase


class TestGetSettingsFile(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.monkeypatch.setattr(dedupe, 'DEDUPE_CONFIG_BUCKET', 'local')

    def test_it_returns_none_on_empty_file(self):
        with dedupe.get_file('asdfasdf', 'rb') as f:
            self.assertIsNone(f)

    def test_it_creates_new_file(self):
        with dedupe.get_file('asdfasdfasdf', 'wb') as f:
            self.assertIsNotNone(f)
            os.remove(os.path.join(BASE_DIR, '../', 'asdfasdfasdf'))

    def test_it_returns_existing_file(self):
        f = open(os.path.join(BASE_DIR, '../','asdfasdfasdf2'), 'wt')
        f.close()
        with dedupe.get_file('asdfasdfasdf2', 'rt') as f:
            self.assertIsNotNone(f)
        os.remove(os.path.join(BASE_DIR, '../', 'asdfasdfasdf2'))


