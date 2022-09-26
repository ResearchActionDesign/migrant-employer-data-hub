import rollbar

from app import settings

token = settings.ROLLBAR_KEY
rollbar.init(token, settings.ENVIRONMENT)
