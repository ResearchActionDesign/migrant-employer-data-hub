import os

os.putenv("ENVIRONMENT", "local")

from app import settings  # noqa


def pytest_configure(config):
    settings.DB_ENGINE = "sqlite"
    settings.ENVIRONMENT = "local"
