from alembic import command  # noqa
from alembic.config import Config  # noqa

from app.settings import ALEMBIC_CONFIG_PATH


def migrate_db():
    alembic_cfg = Config(ALEMBIC_CONFIG_PATH)
    command.upgrade(alembic_cfg, "head")


if __name__ == "__main__":
    migrate_db()
