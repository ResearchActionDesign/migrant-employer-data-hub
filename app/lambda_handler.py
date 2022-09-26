from settings.rollbar import rollbar

from app import actions


@rollbar.lambda_function
def lambda_handler(event, context):
    command = event.get("command", None)
    args = event.get("args", [])

    if command == "scrape_listings":
        return actions.scrape_listings.scrape_listings(**args)

    if command == "scrape_rss":
        return actions.scrape_rss.scrape_rss(**args)

    if command == "import_disclosure":
        return actions.import_disclosure.import_disclosure(**args)

    if command == "update_employer_records":
        return actions.update_employer_records.update_employer_records()

    print("Command not found")
    return None
