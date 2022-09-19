from settings.rollbar import rollbar

import actions


@rollbar.lambda_function
def lambda_handler(event, context):
    command = event.get('command', None)
    args = event.get('args', [])

    if command == 'scrape_listings':
        return actions.scrape_listings.scrape_listings(**args)

    if command == 'scrape_rss':
        return actions.scrape_rss.scrape_rss(**args)

    print('Command not found')
    return
