from sqlmodel import select

from app.actions.dedupe import generate_canonical_employers_from_clusters
from app.db import get_mock_engine
from app.models.base import DoLDataSource
from app.models.dedupe_entity_map import DedupeEntityMap
from app.models.employer_record import EmployerRecord
from app.models.unique_employer import UniqueEmployer
from app.tests.base_test_case import BaseTestCase


class TestGenerateCanonicalEmployersFromClusters(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.monkeypatch.setattr(generate_canonical_employers_from_clusters, 'get_engine', get_mock_engine)

    def test_canonicalize(self):
        a = {
            'key1': 'Value1',
            'key2': 'Value2',
            'key3': None,
        }

        b = {
            'key1': 'value1',
            'key2': 'value2',
            'key3': 'value3',
        }

        c = {
            'key1': 'VALUE1',
            'key2': 'VALUE2',
        }

        # For 2 records it should always prefer the first record.
        canonical_1 = generate_canonical_employers_from_clusters.canonicalize([a, b])
        self.assertEqual('Value1', canonical_1['key1'])
        self.assertEqual('Value2', canonical_1['key2'])
        self.assertEqual('value3', canonical_1['key3'])

        # For these 3 records, record a should be the canonical one.
        canonical_2 = generate_canonical_employers_from_clusters.canonicalize([a, b, c])
        self.assertEqual('Value1', canonical_2['key1'])
        self.assertEqual('Value2', canonical_2['key2'])

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
                trade_name_dba='Test trade name 2',
                source=DoLDataSource.dol_disclosure
            )]


        for e in employer_records:
            self.session.add(e)

        self.session.commit()

        for e in employer_records:
            self.session.refresh(e)

        # Now make them as if they were clustered with high confidence, but one with low.
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

            DedupeEntityMap(
                canon_id=1,
                employer_record_id=employer_records[2].id,
                cluster_score=0.5
            )
        ]
        for c in clusters:
            self.session.add(c)
        self.session.commit()

        generate_canonical_employers_from_clusters.generate_canonical_employers_from_clusters()

        # One new unique employer exists
        employer = self.session.exec(select(UniqueEmployer)).one_or_none()
        self.assertIsNotNone(employer)
        self.assertEqual('Test name', employer.name)
        self.assertEqual('Test trade name', employer.trade_name_dba)

        # Clusters are marked as processed
        for c in clusters:
            self.session.refresh(c)

        self.assertTrue(clusters[0].processed_to_canonical_employer)
        self.assertTrue(clusters[1].processed_to_canonical_employer)
        self.assertFalse(clusters[2].processed_to_canonical_employer)

        for e in employer_records:
            self.session.refresh(e)

        self.assertEqual(employer.id, employer_records[0].unique_employer_id)
        self.assertEqual(employer.id, employer_records[1].unique_employer_id)
        self.assertIsNone(employer_records[2].unique_employer_id)
