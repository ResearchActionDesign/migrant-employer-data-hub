from app.actions.dedupe import generate_canonical_employers_from_clusters
from app.settings import ROLLBAR_ENABLED

if ROLLBAR_ENABLED:
    from app.settings import rollbar


def lambda_handler(event, context=None):
    try:
        generate_canonical_employers_from_clusters.generate_canonical_employers_from_clusters(
            100
        )

        if ROLLBAR_ENABLED:
            return rollbar.wait(lambda: True)

    except:  # noqa
        if ROLLBAR_ENABLED:
            rollbar.report_exc_info()
            rollbar.wait()
            raise

        raise

    return None
