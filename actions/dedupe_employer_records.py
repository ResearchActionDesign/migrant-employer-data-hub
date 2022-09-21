"""
Dedupe records using dedupe package.

This part of the code uses raw sqlalchemy rather than SQLModel in order to be able to avoid some of the overhead required.
@see https://dedupeio.github.io/dedupe-examples/docs/pgsql_big_dedupe_example.html
"""

import os
from typing import Iterable

import dedupe
from dedupe.api import DedupeMatching
from sqlalchemy import (
    Column,
    Float,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    insert,
    select,
)

from actions import initialize_db  # noqa
from db import get_engine
from models.employer_record import EmployerRecord


def employer_record_pairs(result_set: Iterable):
    for _, row in enumerate(result_set):
        employer_a = (
            row.left_id,
            {
                "name": row.name,
                "trade_name_dba": row.trade_name_dba,
                "city": row.city,
                "state": row.state,
                "country": row.country,
                "phone": row.phone,
            },
        )
        employer_b = (
            row.right_id,
            {
                "name": row.name_1,
                "trade_name_dba": row.trade_name_dba_1,
                "city": row.city_1,
                "state": row.state_1,
                "country": row.country_1,
                "phone": row.phone_1,
            },
        )

        yield employer_a, employer_b


def cluster_ids(clustered_dupes):
    for cluster, scores in clustered_dupes:
        cluster_id = cluster[0]
        for employer_id, score in zip(cluster, scores):
            yield {
                "employer_id": int(employer_id),
                "canon_id": int(cluster_id),
                "cluster_score": float(score),
            }


def interactively_train_model() -> DedupeMatching:
    settings_file = "pgsql_big_dedupe_example_settings"
    training_file = "pgsql_big_dedupe_example_training.json"

    # Load settings and configure dedupe object
    if os.path.exists(settings_file):
        with open(settings_file, "rb") as sf:
            deduper = dedupe.StaticDedupe(sf, num_cores=4)
            return deduper

    fields = [
        {"field": "name", "type": "Name"},
        {"field": "trade_name_dba", "type": "Name", "has_missing": True},
        {"field": "city", "type": "Exact"},
        {"field": "state", "type": "Exact"},
        {"field": "country", "type": "Exact", "has_missing": True},
        {"field": "phone", "type": "Exact"},
    ]
    deduper = dedupe.Dedupe(fields, num_cores=4)

    # Yield_per execution option forces use of a server-side cursor, 1,000 is the number of results to buffer in memory.
    engine = get_engine(yield_per=1000)
    conn = engine.connect()

    employer_record_table = Table(
        "employer_record", EmployerRecord.metadata, autoload_with=engine
    )
    # Load results.
    employers = conn.execute(
        select(
            employer_record_table.c.id,
            employer_record_table.c.name,
            employer_record_table.c.trade_name_dba,
            employer_record_table.c.city,
            employer_record_table.c.state,
            employer_record_table.c.country,
            employer_record_table.c.phone,
        ).order_by(employer_record_table.c.id)
    )
    data_set = {
        # Need to cast the row objects to a dictionary for dedupe to recognize them, annoyingly!
        i: row._asdict()
        for i, row in enumerate(employers)
    }

    print(data_set[0])

    if os.path.exists(training_file):
        print("reading labeled examples from ", training_file)
        with open(training_file, "rt") as tf:
            deduper.prepare_training(data_set, tf)
    else:
        deduper.prepare_training(data_set)

    del data_set

    dedupe.console_label(deduper)

    with open(training_file, "wt") as tf:
        deduper.write_training(tf)

    deduper.train(recall=0.90)
    with open(settings_file, "wb") as sf:
        deduper.write_settings(sf)
    deduper.cleanup_training()
    conn.close()

    return deduper


def block_records(deduper: DedupeMatching) -> DedupeMatching:
    engine = get_engine(refresh=True, yield_per=1000)
    conn = engine.connect()

    # Create blocking map table
    metadata_obj = MetaData()
    blocking_map_table = Table(
        "dedupe_blocking_map",
        metadata_obj,
        Column("block_key", String),
        Column("employer_id", Integer),
    )
    metadata_obj.drop_all(
        engine,
        tables=[
            blocking_map_table,
        ],
    )
    metadata_obj.create_all(
        engine,
        tables=[
            blocking_map_table,
        ],
    )

    # Create inverted index
    employer_record_table = Table(
        "employer_record", EmployerRecord.metadata, autoload_with=engine
    )
    for field in deduper.fingerprinter.index_fields:
        print(f"Loading data for {field}")
        field_data = conn.execute(
            select(getattr(employer_record_table.c, field)).distinct()
        ).scalars()
        deduper.fingerprinter.index(field_data, field)

    # Write blocking map
    employers = conn.execute(
        select(employer_record_table).order_by(employer_record_table.c.id)
    )
    full_data = ((row._mapping["id"], row._mapping) for row in employers)  # noqa
    blocking_data = [
        {"block_key": v1, "employer_id": v2}
        for (v1, v2) in deduper.fingerprinter(full_data)
    ]

    # TODO: The example online uses a bulk CSV insert here to speed up things. Is this necessary?
    conn.execute(insert(blocking_map_table), blocking_data)
    conn.commit()

    # This just frees up memory
    deduper.fingerprinter.reset_indices()

    # Now add index on the blocking key.
    blocking_map_idx = Index(
        "blocking_map_idx",
        blocking_map_table.c.block_key,
        blocking_map_table.c.employer_id,
        postgresql_ops={"block_key": "text_pattern_ops", "employer_id": "int4_ops"},
    )
    blocking_map_idx.create(bind=engine)

    # Create entity map table.
    entity_map_table = Table(
        "dedupe_entity_map",
        metadata_obj,
        Column("employer_id", Integer, primary_key=True),
        Column("canon_id", Integer),
        Column("cluster_score", Float),
    )
    metadata_obj.drop_all(
        engine,
        tables=[
            entity_map_table,
        ],
    )
    metadata_obj.create_all(
        engine,
        tables=[
            entity_map_table,
        ],
    )

    # Actually do clustering!
    blocking_map_left = blocking_map_table.alias()
    blocking_map_right = blocking_map_table.alias()
    blocking_map_subquery = (
        select(
            blocking_map_left.c.employer_id.label("left_id"),
            blocking_map_right.c.employer_id.label("right_id"),
        )
        .join_from(
            blocking_map_left,
            blocking_map_right,
            blocking_map_left.c.block_key == blocking_map_right.c.block_key,
        )
        .where(blocking_map_left.c.employer_id < blocking_map_right.c.employer_id)
        .subquery()
    )

    employer_record_table_left = employer_record_table.alias("a")
    employer_record_table_right = employer_record_table.alias("b")
    clustering_query = (
        select(
            blocking_map_subquery,
            employer_record_table_left,
            employer_record_table_right,
        )
        .join_from(
            blocking_map_subquery,
            employer_record_table_left,
            employer_record_table_left.c.id == blocking_map_subquery.c.left_id,
        )
        .join_from(
            blocking_map_subquery,
            employer_record_table_right,
            employer_record_table_right.c.id == blocking_map_subquery.c.right_id,
        )
    )

    clusters_records = conn.execute(clustering_query)
    clustered_dupes = deduper.cluster(
        deduper.score(employer_record_pairs(clusters_records)), threshold=0.5
    )

    # Write out results
    [  # noqa
        conn.execute(insert(entity_map_table), i) for i in cluster_ids(clustered_dupes)
    ]
    conn.commit()
    conn.close()


if __name__ == "__main__":
    deduper_obj = interactively_train_model()
    block_records(deduper_obj)
