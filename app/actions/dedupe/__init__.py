import os
from contextlib import contextmanager
from tempfile import mkstemp
from typing import IO, Any, Generator, Union

import boto3
from botocore import exceptions
from sqlalchemy import Table
from sqlalchemy.future import Engine

from app.models.dedupe_entity_map import DedupeEntityMap
from app.models.employer_record import EmployerRecord
from app.settings import BASE_DIR, DEDUPE_CONFIG_BUCKET, DEDUPE_CONFIG_FILE_PREFIX

# TODO: Load these from somewhere stable -- S3?
settings_file = DEDUPE_CONFIG_FILE_PREFIX + "cdm_dedupe_settings"
training_file = DEDUPE_CONFIG_FILE_PREFIX + "cdm_dedupe_training.json"


def get_employer_record_table(engine: Engine) -> Table:
    if not hasattr(get_employer_record_table, "employer_record_table"):
        get_employer_record_table.employer_record_table = Table(
            "employer_record", EmployerRecord.metadata, autoload_with=engine
        )
    return get_employer_record_table.employer_record_table


def get_cluster_table(engine: Engine) -> Table:
    if not hasattr(get_cluster_table, "table"):
        get_cluster_table.table = Table(
            "dedupe_entity_map", DedupeEntityMap.metadata, autoload_with=engine
        )
    return get_cluster_table.table


@contextmanager
def get_file(
    filename: str, mode: str = "rt"
) -> Generator[Union[IO[Any], None], None, None]:
    if DEDUPE_CONFIG_BUCKET == "local":
        full_local_path = os.path.join(BASE_DIR, "../", filename)
        if os.path.exists(full_local_path) or mode[0] == "w":
            file = open(full_local_path, mode)
            yield file
            file.close()
        else:
            yield None
        return

    boto3_session = boto3.session.Session(
        profile_name=os.getenv("AWS_PROFILE_NAME", None)
    )
    s3 = boto3_session.resource("s3")
    f = None

    f, temp_filename = mkstemp()
    file_handle = None
    try:
        if mode[0] == "w":
            file_handle = open(f, mode)
            yield file_handle
            file_handle.close()
            s3.Bucket(DEDUPE_CONFIG_BUCKET).upload_file(temp_filename, filename)
        elif mode[0] == "r":
            s3.Bucket(DEDUPE_CONFIG_BUCKET).Object(filename).download_file(
                temp_filename
            )
            file_handle = open(temp_filename, mode)
            yield file_handle
        else:
            yield None
    except exceptions.ClientError:
        yield None
    finally:
        if file_handle:
            file_handle.close()
        os.remove(temp_filename)
    return
