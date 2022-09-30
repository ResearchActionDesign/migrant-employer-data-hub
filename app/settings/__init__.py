import os
import re

import rollbar
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
DEDUPE_CONFIG_BUCKET = os.getenv("DEDUPE_CONFIG_BUCKET", "local")
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROLLBAR_KEY = os.getenv("ROLLBAR_KEY", "missing_api_key")
ENVIRONMENT = os.getenv("ENVIRONMENT", "local")
ROWS_BEFORE_COMMIT = 100

SQLITE_FILE_NAME = "test_database.db"
DB_URL = f"sqlite:///{BASE_DIR}/../{SQLITE_FILE_NAME}"
DB_ENGINE = os.getenv("DB_ENGINE", "sqlite")

ALEMBIC_CONFIG_PATH = f"{BASE_DIR}/../alembic.ini"

token = ROLLBAR_KEY
ROLLBAR_ENABLED = False

if token != "missing_api_key":
    rollbar.init(token, ENVIRONMENT)
    ROLLBAR_ENABLED = True

# Model parameters
TRAINING_SAMPLE_SIZE = int(
    os.getenv("TRAINING_SAMPLE_SIZE", "10000")
)  # Number of records to pull from DB for training.
TRAINING_RECALL_PERCENT = 0.9
DEDUPE_CLUSTERING_THRESHOLD = 0.6
DEDUPE_CLUSTER_REVIEW_THRESHOLD = (
    0.8  # Anything below this threshold or at it gets reviewed.
)
DEDUPE_CONFIG_FILE_PREFIX = os.getenv("DEDUPE_CONFIG_FILE_PREFIX", "")
