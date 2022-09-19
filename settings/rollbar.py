import settings
import rollbar

token = settings.ROLLBAR_KEY
rollbar.init(token, settings.ENVIRONMENT)
