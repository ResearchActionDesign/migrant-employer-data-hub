import sys
from datetime import datetime
from time import strftime

import feedparser
import rollbar
from sqlmodel import Session, select

from constants import USER_AGENT_STRING
from db import get_engine
from models.base import DoLDataSource
from models.dol_disclosure_job_order import DolDisclosureJobOrder  # noqa
from models.seasonal_jobs_job_order import SeasonalJobsJobOrder
from models.static_value import StaticValue
from settings import DOL_ID_REGEX, ETAG_KEY, JOBS_RSS_FEED_URL, MODIFIED_KEY


def scrape_rss(max_records: int = -1, skip_update: bool = False):
    """
    Scrape Seasonaljobs.dol.gov RSS feed for new job listings.

    :param max_records: Max number of entries to process, defaults to -1 (all)
    :param skip_update: Skip updating existing records if found?
    :return:
    """
    if not JOBS_RSS_FEED_URL:
        raise Exception("RSS feed URL must be set")

    session = Session(get_engine())

    # Check for saved etag and modified keys
    try:
        etag_obj = session.exec(
            select(StaticValue).where(StaticValue.key == ETAG_KEY)
        ).first()
        etag = etag_obj.value
    except AttributeError:
        etag_obj = StaticValue(key=ETAG_KEY)
        etag = None

    try:
        modified_obj = session.exec(
            select(StaticValue).where(StaticValue.key == MODIFIED_KEY)
        ).first()
        modified = modified_obj.value
    except AttributeError:
        modified_obj = StaticValue(key=MODIFIED_KEY)
        modified = None

    rss_entries = feedparser.parse(
        JOBS_RSS_FEED_URL,
        etag=etag,
        modified=modified,
        agent=USER_AGENT_STRING,
    )

    if rss_entries.get("bozo", False):
        # Error code from feed scraper
        msg = f"Error pulling RSS Feed {rss_entries.get('bozo_exception', '')}"

        sys.stderr.write(msg)
        rollbar.report_message(msg, "error")

    if rss_entries.get("status", False) not in [200, 301]:
        # Error code from feed scraper
        msg = f"RSS Feed status code: {rss_entries.get('status', False)}, not 200"
        sys.stderr.write(msg)
        rollbar.report_message(msg, "error")
        return

    if rss_entries.get("version", "") == "":
        print("RSS fetched, but no new entries")
        return

    processed_count = 0
    for entry in rss_entries.get("entries", []):
        processed_count += 1
        if processed_count > max_records > 0:
            break

        link = entry.get("link", "")
        dol_ids = DOL_ID_REGEX.findall(link)

        if len(dol_ids) == 0:
            msg = f'No Dol ID found in RSS listing, with link="{link}"'
            rollbar.report_message(msg, "error")
            continue
        if len(dol_ids) > 1:
            msg = f'Multiple Dol IDs found in RSS listing, with link="{link}"'
            rollbar.report_message(msg, "error")
            continue

        dol_id = dol_ids[0]
        pub_date = strftime("%Y-%m-%d", entry.get("published_parsed", ""))
        if not dol_id:
            sys.stderr.write(f"Invalid entry with link {entry.get('link', 'N/A')}")
            continue

        listing_attributes = {
            "link": entry.get("link", ""),
            "title": entry.get("title", ""),
            "description": entry.get("description", ""),
            "pub_date": pub_date,
            "last_seen": datetime.now(),
        }

        existing_listing = session.exec(
            select(SeasonalJobsJobOrder).where(SeasonalJobsJobOrder.dol_id == dol_id)
        ).first()

        if existing_listing and skip_update:
            print(
                f"{processed_count} skipped updating entry with title {listing_attributes['title']} and id {dol_id}"
            )
            continue

        operation = None
        if not existing_listing:
            operation = "Created"
            session.add(
                SeasonalJobsJobOrder(
                    source=DoLDataSource.scraper, dol_id=dol_id, **listing_attributes
                )
            )

        else:
            operation = "Updated"
            for k, v in listing_attributes.items():
                setattr(existing_listing, k, v)
            session.add(existing_listing)

        print(
            f"{processed_count} - {operation} entry with title {listing_attributes['title']} and id {dol_id}"
        )

    # Assuming scrape was successful, save etag and last_modified.
    if rss_entries.get("etag", False):
        etag_obj.value = rss_entries.get("etag", "")
        session.add(etag_obj)

    if rss_entries.get("modified", False):
        modified_obj.value = rss_entries.get("modified", "")
        session.add(modified_obj)

    session.commit()
    session.close()


if __name__ == "__main__":
    scrape_rss(10)
