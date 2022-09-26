from datetime import datetime
from unittest import TestCase
from unittest.mock import MagicMock

import pytest
import requests
from sqlmodel import Session, select

from actions import scrape_listings
from db import drop_all_models, get_mock_engine
from models.base import DoLDataSource
from models.seasonal_jobs_job_order import SeasonalJobsJobOrder


class FakeResponse(object):
    # default response attributes
    status_code = 200
    content = b"Some content"
    url = "https://seasonaljob.dol.gov/a-real-url"

    def json(self):
        return {"value": [{"a_key": "a value", "case_number": "H-1"}]}

    def invalid_json(self):
        raise ValueError


class TestScrapeListings(TestCase):
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

        # currently just creates 1 listing
        for i in range(1, 2):
            self.session.add(SeasonalJobsJobOrder(
                dol_id=f"H-{i}",
                link=f"http://seasonaljobs.dol.gov/jobs/H-{i}",
                title=f"Test title #{i}",
                description="Test description",
                pub_date=datetime.now(),
                source=DoLDataSource.scraper,
            ))
        self.session.commit()
        self.monkeypatch.setattr(scrape_listings, 'get_engine', get_mock_engine)

    def tearDown(self):
        drop_all_models()
        self.session.close()

    def test_fails_on_invalid_status_code(self):
        mock_request_post = MagicMock()
        mock_request_post.return_value = FakeResponse()
        mock_request_post.return_value.status_code = 403
        self.monkeypatch.setattr(requests, 'post', mock_request_post)

        scrape_listings.scrape_listings()
        mock_request_post.assert_called_once()

        self.assertEqual(len(self.session.exec(select(SeasonalJobsJobOrder).where(SeasonalJobsJobOrder.scraped == True)).all()), 0)
        self.assertIn("API call failed for listing", self.capsys.readouterr().err)

    def test_fails_on_invalid_json(self):
        mock_request_post = MagicMock()
        mock_request_post.return_value = FakeResponse()
        mock_request_post.return_value.json = (
            mock_request_post.return_value.invalid_json
        )
        self.monkeypatch.setattr(requests, 'post', mock_request_post)

        scrape_listings.scrape_listings(1)
        mock_request_post.assert_called_once()
        self.assertEqual(len(self.session.exec(select(SeasonalJobsJobOrder).where(SeasonalJobsJobOrder.scraped== True)).all()), 0)
        self.assertIn("Invalid JSON", self.capsys.readouterr().err)

    def test_successfully_scrapes_one_listing(self):
        mock_request_post = MagicMock()
        mock_request_get = MagicMock()
        mock_request_post.return_value = FakeResponse()
        mock_request_get.return_value = FakeResponse()
        self.monkeypatch.setattr(requests, 'post', mock_request_post)
        self.monkeypatch.setattr(requests, 'get', mock_request_get)

        scrape_listings.scrape_listings(max_records=1)
        mock_request_post.assert_called_once()
        self.assertIn("H-1", self.capsys.readouterr().out)
        self.assertEqual('', self.capsys.readouterr().err)
        scraped_listings = self.session.exec(select(SeasonalJobsJobOrder).where(SeasonalJobsJobOrder.scraped==True)).all()
        self.assertEqual(len(scraped_listings), 1)
        self.assertTrue(scraped_listings[0].scraped)
        self.assertIn("a_key", scraped_listings[0].scraped_data)
        self.assertEqual(scraped_listings[0].scraped_data["a_key"], "a value")

    # def test_successfully_saves_pdf(self):
    #     with patch("requests.post") as mock_request_post, patch(
    #         "requests.get"
    #     ) as mock_request_get:
    #         mock_request_post.return_value = FakeResponse()
    #         mock_request_get.return_value = FakeResponse()
    #         out = StringIO()
    #         call_command("scrape_listings", stdout=out, max=1)
    #         mock_request_get.assert_called_once()
    #         self.assertIn("Saved job order PDF", out.getvalue())
    #         self.assertEqual(Listing.objects.filter(scraped=True).count(), 1)
    #         l = Listing.objects.get(dol_id="H-1")
    #         self.assertIsNotNone(l.pdf)
    #         self.assertEqual(l.pdf.readline(), b"Some content")
