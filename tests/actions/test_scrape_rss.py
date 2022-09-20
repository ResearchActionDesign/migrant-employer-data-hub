import time
from unittest import TestCase
from unittest.mock import patch

import pytest
from sqlmodel import Session, select

from actions import scrape_rss
from db import drop_all_models, get_mock_engine
from models.seasonal_jobs_job_order import SeasonalJobsJobOrder
from models.static_value import StaticValue


class TestScrapeRSS(TestCase):
    session: Session|None = None

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        self.capsys = capsys

    @pytest.fixture(autouse=True)
    def monkeypatch(self, monkeypatch):
        self.monkeypatch = monkeypatch

    def setUp(self):
        engine = get_mock_engine()
        self.session = Session(engine)
        self.monkeypatch.setattr(scrape_rss, 'get_engine', get_mock_engine)

    def tearDown(self):
        drop_all_models()

    def test_fails_on_bozo_error(self):
        with patch("feedparser.parse") as mock_parse:
            mock_parse.return_value = {"bozo": 1, "bozo_exception": "Test exception"}
            self.assertFalse(scrape_rss.scrape_rss())
            output = self.capsys.readouterr()
            self.assertIn("Error", output.err)
            self.assertIn("Test exception", output.err)
            self.assertEqual(0,
                len(self.session.exec(select(SeasonalJobsJobOrder)).all()),
                )

    def test_fails_on_invalid_status_code(self):
        with patch("feedparser.parse") as mock_parse:
            mock_parse.return_value = {
                "status": 403,
            }
            scrape_rss.scrape_rss()
            self.assertIn("403", self.capsys.readouterr().err)
            self.assertEqual(0,
                len(self.session.exec(select(SeasonalJobsJobOrder)).all()),
                )

    def test_successfully_handles_no_new_entries(self):
        with patch("feedparser.parse") as mock_parse:
            mock_parse.return_value = {
                "status": 200,
                "version": "",
            }
            scrape_rss.scrape_rss()
            self.assertIn("no new entries", self.capsys.readouterr().out)
            self.assertEqual(0,
                len(self.session.exec(select(SeasonalJobsJobOrder)).all()),
                )

    def test_successfully_scrapes_entries(self):
        with patch("feedparser.parse") as mock_parse:
            test_entries = [
                {
                    "link": f"http://seasonaljobs.dol.gov/jobs/H-{n}",
                    "title": f"Test title #{n}",
                    "description": "Test description",
                    "published_parsed": time.localtime(),
                }
                for n in range(1, 6)
            ]
            mock_parse.return_value = {
                "status": 200,
                "version": "test",
                "entries": test_entries,
            }

            scrape_rss.scrape_rss()
            output = self.capsys.readouterr()
            self.assertIn("Test title #1", output.out)
            self.assertIn("Test title #5", output.out)
            self.assertEqual(5,
                len(self.session.exec(select(SeasonalJobsJobOrder)).all()),
                )

    def test_saves_modified_date_and_etag(self):
        with patch("feedparser.parse") as mock_parse:
            mock_parse.return_value = {
                "status": 200,
                "version": "test",
                "entries": [],
                "etag": "6c132-941-ad7e3080",
                "modified": "Fri, 11 Jun 2012 23:00:34 GMT",
            }
            scrape_rss.scrape_rss()
            self.assertEqual(0,
                len(self.session.exec(select(SeasonalJobsJobOrder)).all()),
                )
            etag = self.session.exec(select(StaticValue).where(StaticValue.key == 'jobs_rss__etag')).one()
            modified = self.session.exec(select(StaticValue).where(StaticValue.key == 'jobs_rss__modified')).one()
            self.assertEqual(etag.value, "6c132-941-ad7e3080")
            self.assertEqual(modified.value, "Fri, 11 Jun 2012 23:00:34 GMT")
