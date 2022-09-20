import random
import sys

import requests
import rollbar
from sqlalchemy import false
from sqlalchemy.exc import MultipleResultsFound, NoResultFound
from sqlmodel import Session, select

import settings
from constants import USER_AGENT_STRING
from db import get_engine
from models.dol_disclosure_job_order import DolDisclosureJobOrder  # noqa
from models.seasonal_jobs_job_order import SeasonalJobsJobOrder


def scrape_listings(max_records: int = 1):
    """
    Scrape a detailed listing from SeasonalJobs azure server.

    :param max_records: Number of records to scrape, defaults to 1
    :return:
    """

    if not (settings.JOBS_API_URL and settings.JOB_ORDER_BASE_URL):
        raise Exception("JOBS_API_URL and JOB_ORDER_BASE_URL must be set")
    if not settings.JOBS_API_KEY:
        raise Exception("Jobs API Key must be set")

    session = Session(get_engine())

    unscraped_listings = session.exec(
        select(SeasonalJobsJobOrder)
        .where(SeasonalJobsJobOrder.scraped == false())
        .order_by(SeasonalJobsJobOrder.first_seen.desc())
        .limit(max_records)
    ).all()

    if len(unscraped_listings) == 0:
        print("No listings left to scrape!")
        return

    scraped_count = 0
    for listing in unscraped_listings:
        if scraped_count >= max_records:
            break

        payload = {
            "searchFields": "case_number",
            "orderby": "search.score() desc",
            "search": f'"{listing.dol_id}"',
            "top": 1,
        }
        api_response = requests.post(
            settings.JOBS_API_URL,
            json=payload,
            headers={
                "User-Agent": USER_AGENT_STRING,
                "Content-Type": "application/json",
            },
            timeout=30,
        )
        if api_response.status_code != 200:
            msg = f"API call failed for listing, status code {api_response.status_code}"
            rollbar.report_message(
                msg,
                "error",
                extra_data={
                    "dol_id": listing.dol_id,
                },
            )
            sys.stderr.write(msg)
            continue

        try:
            scraped_data = api_response.json()["value"][0]
        except ValueError:
            msg = "Invalid JSON"
            sys.stderr.write(msg)
            rollbar.report_message(
                msg,
                "error",
                extra_data={"dol_id": listing.dol_id, "response": api_response},
            )
            continue

        scrape_successful = True
        if scraped_data["case_number"] != listing.dol_id:
            msg = (
                f"Case number mismatch between scraped data for DOL ID {listing.dol_id}. "
                f"Scraped URL {settings.JOBS_API_URL}"
            )
            print(msg)

            # Try to parse the data anyway.
            original_listing = listing
            try:
                listing = session.exec(
                    select(SeasonalJobsJobOrder).where(
                        SeasonalJobsJobOrder.dol_id == scraped_data["case_number"],
                        SeasonalJobsJobOrder.scraped == false(),
                    )
                ).one()

            except (NoResultFound, MultipleResultsFound):
                listing = original_listing  # Save this value so we can check for a PDF
                scrape_successful = False

        if scrape_successful:
            listing.scraped = True
            listing.scraped_data = scraped_data
            listing.clean()

            session.add(listing)
            scraped_count += 1
            print(f"{scraped_count} - Saved data for listing ID {listing.dol_id}")
            session.commit()

        if listing.pdf:
            continue

        headers = {
            "User-Agent": USER_AGENT_STRING,
        }
        pdf_url = f"{settings.JOB_ORDER_BASE_URL}{listing.dol_id}"
        job_order_pdf = requests.get(
            pdf_url,
            headers=headers,
            timeout=30,
        )

        if (
            job_order_pdf.status_code in (200, 301)
            and job_order_pdf.url != "https://seasonaljobs.dol.gov/system/404"
        ):
            True  # noqa
            # TODO: Scrape PDF.
            # listing.pdf = ContentFile(
            #     job_order_pdf.content, name=f"{listing.dol_id}.pdf"
            # )
            # listing.save()
            # self.stdout.write(
            #     self.style.SUCCESS(
            #         f"{scraped_count} - Saved job order PDF for listing ID {listing.dol_id}"
            #     )
            # )

        else:
            # We want to track in rollbar if there's a spike in this error because maybe then
            # PDF scraping is broken entirely, but don't need to track every single occurance,
            # so throttling to only log 1/10 of the occurences.
            if random.randint(0, 10) == 0:
                rollbar.report_message(
                    "Failed job order PDF request for listing ID",
                    "warning",
                    extra_data={
                        "dol_id": listing.dol_id,
                        "pdf_request": job_order_pdf,
                        "pdf_url": pdf_url,
                    },
                )
            sys.stderr.write(
                f"{scraped_count} - Failed job order PDF request for listing ID {listing.dol_id}, url {pdf_url}"
            )
    session.close()


if __name__ == "__main__":
    scrape_listings(5)
