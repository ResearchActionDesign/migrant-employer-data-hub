import urllib

from app.actions import import_disclosure
from app.settings import ROLLBAR_ENABLED

if ROLLBAR_ENABLED:
    from app.settings import rollbar


def lambda_handler(event, context=None):
    result = None

    try:
        # If this has been triggered from S3, there will be a 'records' param on the event.
        records = event.get("Records", None)

        if records:
            for record in records:
                bucket_name = record["s3"]["bucket"]["name"]
                object_name = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
                import_disclosure.add_new_import(
                    object_name=object_name, bucket_name=bucket_name
                )

        if ROLLBAR_ENABLED:
            return rollbar.wait(lambda: result)

        return result

    except:  # noqa
        if ROLLBAR_ENABLED:
            rollbar.report_exc_info()
            rollbar.wait()
            raise

        raise
