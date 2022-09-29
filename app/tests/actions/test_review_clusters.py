from typing import Union
from unittest import TestCase
from unittest.mock import patch

import pytest
from sqlmodel import Session, select

from app.actions.dedupe import review_clusters
from app.db import drop_all_models, get_mock_engine
from app.models.base import DoLDataSource
from app.models.dedupe_entity_map import DedupeEntityMap
from app.models.employer_record import EmployerRecord

# flake8: noqa

class TestReviewClusters(TestCase):
    session: Union[Session, None] = None

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        self.capsys = capsys

    def setUp(self):
        engine = get_mock_engine()
        self.session = Session(engine)
        self.monkeypatch.setattr(review_clusters, 'get_engine', get_mock_engine)

    @pytest.fixture(autouse=True)
    def monkeypatch(self, monkeypatch):
        self.monkeypatch = monkeypatch

    def tearDown(self):
        drop_all_models()
        self.session.close()

    @patch('builtins.input', lambda *args: 'y')
    def test_review_clusters(self):
        # First, generate a few employer records.
        e1 = EmployerRecord(
            name='Test name',
            trade_name_dba='Test trade name',
            source=DoLDataSource.dol_disclosure
        )
        e2 = EmployerRecord(
            name = 'Test name 2',
            trade_name_dba='Test trade name 2',
            source=DoLDataSource.dol_disclosure
        )
        self.session.add(e1)
        self.session.add(e2)
        self.session.commit()
        self.session.refresh(e1)
        self.session.refresh(e2)

        # Now make them as if they were clustered with low confidence
        clusters = [
            DedupeEntityMap(
                canon_id=1,
                employer_record_id=e1.id,
                cluster_score=0.7
            ),

            DedupeEntityMap(
                canon_id=1,
                employer_record_id=e2.id,
                cluster_score=0.7
            )
        ]
        for c in clusters:
            self.session.add(c)
        self.session.commit()

        self.assertEqual(2, len(self.session.exec(select(DedupeEntityMap)).all()))
        review_clusters.review_clusters()

        # Assert output looks right
        output_1 =  \
        """+---+----+-------------+-------------------+------+-------+---------+-------+
| * | ID | Name        | Trade Name        | City | State | Country | Phone |
+---+----+-------------+-------------------+------+-------+---------+-------+
| * | 1  | Test name   | Test trade name   | None | None  | None    | None  |
|   | 2  | Test name 2 | Test trade name 2 | None | None  | None    | None  |
+---+----+-------------+-------------------+------+-------+---------+-------+"""
        output_2 = \
            """+---+----+-------------+-------------------+------+-------+---------+-------+
| * | ID | Name        | Trade Name        | City | State | Country | Phone |
+---+----+-------------+-------------------+------+-------+---------+-------+
|   | 1  | Test name   | Test trade name   | None | None  | None    | None  |
| * | 2  | Test name 2 | Test trade name 2 | None | None  | None    | None  |
+---+----+-------------+-------------------+------+-------+---------+-------+"""

        output = self.capsys.readouterr().out
        self.assertIn(output_1, output)
        self.assertIn(output_2, output)

        # Assert records saved correctly
        for c in clusters:
            self.session.refresh(c)

        self.assertEqual(True, clusters[0].is_valid_cluster)
        self.assertEqual(True, clusters[1].is_valid_cluster)
        self.assertIsNotNone(clusters[0].review_date)
        self.assertIsNotNone(clusters[1].review_date)