import settings


def pytest_configure(config):
    settings.DB_ENGINE = "sqlite"
