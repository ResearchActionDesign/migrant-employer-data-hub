from app.actions import update_addresses
from app.settings import ROLLBAR_ENABLED

if ROLLBAR_ENABLED:
    from app.settings import rollbar


def lambda_handler(event, context=None):
    try:
        update_addresses.update_addresses(250)

        if ROLLBAR_ENABLED:
            return rollbar.wait(lambda: True)

    except:  # noqa
        if ROLLBAR_ENABLED:
            rollbar.report_exc_info()
            rollbar.wait()
            raise

        raise

    return None
