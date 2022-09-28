from app.actions import scrape_rss
from app.settings import ROLLBAR_ENABLED

if ROLLBAR_ENABLED:
    from app.settings import rollbar


def lambda_handler(event, context=None):
    result = None

    try:
        result = scrape_rss.scrape_rss()

        if ROLLBAR_ENABLED:
            return rollbar.wait(lambda: result)

        return result

    except:  # noqa
        if ROLLBAR_ENABLED:
            rollbar.report_exc_info()
            rollbar.wait()
            raise

        raise
