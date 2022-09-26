import rollbar

import settings

token = settings.ROLLBAR_KEY
rollbar.init(token, settings.ENVIRONMENT)
