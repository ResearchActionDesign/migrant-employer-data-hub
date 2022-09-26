import os
import re
import urllib.parse

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

ETAG_KEY = "jobs_rss__etag"
MODIFIED_KEY = "jobs_rss__modified"
DOL_ID_REGEX = re.compile(r"(H-[0-9\-]+)")
JOBS_RSS_FEED_URL = "https://seasonaljobs.dol.gov/job_rss.xml"
JOBS_API_URL = "https://api.seasonaljobs.dol.gov/datahub/search?api-version=2020-06-30"
JOB_ORDER_BASE_URL = "https://api.seasonaljobs.dol.gov/job-order/"
JOBS_API_KEY = os.getenv("JOBS_API_KEY", "test")
JOB_ORDER_PDF_DESTINATION = os.getenv("JOB_ORDER_PDF_DESTINATION", "local")
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROLLBAR_KEY = os.getenv("ROLLBAR_KEY", "missing_api_key")
ENVIRONMENT = os.getenv("ENVIRONMENT", "local")
ROWS_BEFORE_COMMIT = 100

SQLITE_FILE_NAME = "test_database.db"
DB_URL = f"sqlite:///{BASE_DIR}/{SQLITE_FILE_NAME}"
DB_ENGINE = os.getenv("DATABASE_ENGINE", "sqlite")

if DB_ENGINE == "postgres" and ENVIRONMENT == "lambda":
    import boto3

    session = boto3.Session(profile_name="cdm")
    client = session.client("rds")
    token = client.generate_db_auth_token(
        DBHostname=os.getenv("RDS_HOSTNAME"),
        Port=5432,
        DBUsername=os.getenv("POSTGRES_USER", "postgres"),
        Region=os.getenv("AWS_REGION", ""),
    )
    DB_URL = (
        f"postgresql://{os.getenv('POSTGRES_USER', 'postgres')}:"
        f"{urllib.parse.quote_plus(token)}@{os.getenv('RDS_HOSTNAME')}:5432/postgres"
    )

elif DB_ENGINE == "postgres":
    DB_URL = (
        f"postgresql://{os.getenv('POSTGRES_USER', 'postgres')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'postgres')}"
        f"@{os.getenv('POSTGRES_URL', '0.0.0.0')}:{os.getenv('POSTGRES_PORT', '15432')}"
    )

ALEMBIC_CONFIG_PATH = f"{BASE_DIR}/alembic.ini"
