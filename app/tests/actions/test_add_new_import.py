from typing import Union
from unittest import TestCase
from unittest.mock import MagicMock

import pytest
from sqlmodel import Session, select

from app.actions import import_disclosure
from app.db import drop_all_models, get_mock_engine
from app.lambda_handlers import add_new_import
from app.models.imported_dataset import ImportedDataset, ImportStatus


class TestAddNewImport(TestCase):
    session: Union[Session, None] = None

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        self.capsys = capsys

    @pytest.fixture(autouse=True)
    def monkeypatch(self, monkeypatch):
        self.monkeypatch = monkeypatch

    def setUp(self):
        engine = get_mock_engine()
        self.session = Session(engine)
        self.monkeypatch.setattr(import_disclosure, 'get_engine', get_mock_engine)

    def tearDown(self):
        drop_all_models()
        self.session.close()

    def test_add_new_import_lambda_handler(self):
       mock_add_new_import = MagicMock()
       self.monkeypatch.setattr(import_disclosure, 'add_new_import', mock_add_new_import)


       add_new_import.lambda_handler(event={
           'Records': [
               {
                   's3': {
                       'bucket': {
                           'name': 'TEST_NAME'
                       },
                        'object': {
                            'key': 'TEST_KEY'
                        }
                   }
               },
               {
                   's3': {
                       'bucket': {
                           'name': 'TEST_NAME_2'
                       },
                       'object': {
                           'key': 'TEST_KEY_2'
                       }
                   }
               }
           ]
       }, context=None)

       mock_add_new_import.assert_any_call(object_name='TEST_KEY', bucket_name='TEST_NAME')
       mock_add_new_import.assert_any_call(object_name='TEST_KEY_2', bucket_name='TEST_NAME_2')

    def test_add_new_import(self):
        self.assertEqual(0, len(self.session.exec(select(ImportedDataset)).all()))
        import_disclosure.add_new_import(object_name='TEST_KEY', bucket_name='TEST_NAME')
        import_disclosure.add_new_import(object_name='TEST_KEY_2', bucket_name='TEST_NAME_2')
        datasets = self.session.exec(select(ImportedDataset).where(ImportedDataset.import_status == ImportStatus.needs_importing).order_by(ImportedDataset.object_name)).all()
        self.assertEqual(2, len(datasets))
        self.assertEqual('TEST_KEY', datasets[0].object_name)
        self.assertEqual('TEST_KEY_2', datasets[1].object_name)

    def test_process_imports(self):
        mock_import_disclosure = MagicMock(return_value=True)
        self.monkeypatch.setattr(import_disclosure, 'import_disclosure', mock_import_disclosure)

        import_disclosure.add_new_import(object_name='TEST_KEY', bucket_name='TEST_NAME')
        import_disclosure.add_new_import(object_name='TEST_KEY_2', bucket_name='TEST_NAME_2')
        self.assertEqual(2, len(self.session.exec(
            select(ImportedDataset).where(ImportedDataset.import_status == ImportStatus.needs_importing).order_by(
                ImportedDataset.object_name)).all()))

        import_disclosure.process_imports()
        mock_import_disclosure.assert_called_once()
        self.assertEqual(1, len(self.session.exec(
            select(ImportedDataset).where(ImportedDataset.import_status == ImportStatus.needs_importing).order_by(
                ImportedDataset.object_name)).all()))
        self.assertEqual(1, len(self.session.exec(
            select(ImportedDataset).where(ImportedDataset.import_status == ImportStatus.finished).order_by(
                ImportedDataset.object_name)).all()))

        mock_import_disclosure.reset_mock(return_value=True)
        import_disclosure.process_imports()
        mock_import_disclosure.assert_called_once()
        self.assertEqual(0, len(self.session.exec(
            select(ImportedDataset).where(ImportedDataset.import_status == ImportStatus.needs_importing).order_by(
                ImportedDataset.object_name)).all()))
        self.assertEqual(2, len(self.session.exec(
            select(ImportedDataset).where(ImportedDataset.import_status == ImportStatus.finished).order_by(
                ImportedDataset.object_name)).all()))