from sqlalchemy import Table

from app.models.dedupe_entity_map import DedupeEntityMap
from app.models.employer_record import EmployerRecord

# TODO: Load these from somewhere stable -- S3?
settings_file = "../pgsql_big_dedupe_example_settings"
training_file = "../pgsql_big_dedupe_example_training.json"


def get_employer_record_table(engine) -> Table:
    if not hasattr(get_employer_record_table, "employer_record_table"):
        get_employer_record_table.employer_record_table = Table(
            "employer_record", EmployerRecord.metadata, autoload_with=engine
        )
    return get_employer_record_table.employer_record_table


def get_cluster_table(engine) -> Table:
    if not hasattr(get_cluster_table, "table"):
        get_cluster_table.table = Table(
            "dedupe_entity_map", DedupeEntityMap.metadata, autoload_with=engine
        )
    return get_cluster_table.table
