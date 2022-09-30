from unittest.mock import MagicMock

from sqlmodel import select

from app.actions.dedupe import generate_canonical_employers_from_non_clustered_records
from app.db import get_mock_engine
from app.models.base import DoLDataSource
from app.models.dedupe_entity_map import DedupeEntityMap
from app.models.employer_record import EmployerRecord
from app.models.unique_employer import UniqueEmployer
from app.tests.base_test_case import BaseTestCase


class TestGenerateCanonicalEmployersFromNonClusteredRecords(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.monkeypatch.setattr(generate_canonical_employers_from_non_clustered_records, 'get_engine', get_mock_engine)

    def test_generate_canonical_employers_from_clusters(self):
        employer_records = [EmployerRecord(
            name='Test name',
            trade_name_dba='Test trade name',
            source=DoLDataSource.dol_disclosure
        ), EmployerRecord(
            name='Test name 2',
            trade_name_dba='Test trade name 2',
            source=DoLDataSource.dol_disclosure
        ),
            EmployerRecord(
                name='Test name 3',
                trade_name_dba='Test trade name 3',
                source=DoLDataSource.dol_disclosure
            )]


        for e in employer_records:
            self.session.add(e)

        self.session.commit()

        for e in employer_records:
            self.session.refresh(e)

        # Now make them as if first two were clustered, third not.
        clusters = [
            DedupeEntityMap(
                canon_id=1,
                employer_record_id=employer_records[0].id,
                cluster_score=0.9
            ),

            DedupeEntityMap(
                canon_id=1,
                employer_record_id=employer_records[1].id,
                cluster_score=0.9
            ),
        ]
        for c in clusters:
            self.session.add(c)
        self.session.commit()

        generate_canonical_employers_from_non_clustered_records.generate_canonical_employers_from_non_clustered_records()

        # One new unique employer exists
        employer = self.session.exec(select(UniqueEmployer)).one_or_none()
        self.assertIsNotNone(employer)
        self.assertEqual('Test name 3', employer.name)
        self.assertEqual('Test trade name 3', employer.trade_name_dba)

        # No clusters are marked as processed
        for c in clusters:
            self.session.refresh(c)

        self.assertFalse(clusters[0].processed_to_canonical_employer)
        self.assertFalse(clusters[1].processed_to_canonical_employer)

        for e in employer_records:
            self.session.refresh(e)

        self.assertEqual(employer.id, employer_records[2].unique_employer_id)
        self.assertIsNone(employer_records[0].unique_employer_id)
        self.assertIsNone(employer_records[1].unique_employer_id)
