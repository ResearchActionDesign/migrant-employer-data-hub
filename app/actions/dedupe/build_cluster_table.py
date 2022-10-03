"""
Dedupe records using dedupe package.

This part of the code uses raw sqlalchemy rather than SQLModel in order to be able to avoid
some overhead required.

@see https://dedupeio.github.io/dedupe-examples/docs/pgsql_big_dedupe_example.html
"""

import os
from typing import Generator, Iterable, Tuple, Union

import dedupe
from sqlalchemy import MetaData, Table, delete, insert, select

from app.actions.dedupe import (
    get_cluster_table,
    get_employer_record_table,
    settings_file,
)
from app.db import get_engine
from app.models.dedupe_blocking_map import DedupeBlockingMap
from app.models.dol_disclosure_job_order import DolDisclosureJobOrder  # noqa
from app.settings import DEDUPE_CLUSTERING_THRESHOLD


def employer_record_pairs(
    result_set: Iterable[object],
) -> Generator[Tuple[Tuple[int, dict], Tuple[int, dict[str, str]]], None, None]:
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


def cluster_ids(clustered_dupes) -> Generator[dict[str, Union[int, float]], None, None]:
    for cluster, scores in clustered_dupes:
        cluster_id = cluster[0]
        for employer_record_id, score in zip(cluster, scores):
            yield {
                "employer_record_id": int(employer_record_id),
                "canon_id": int(cluster_id),
                "cluster_score": float(score),
            }


def build_cluster_table(refresh: bool = False) -> bool:
    """
    Dedupe records based on existing settings file (which must have been created from training set).

    :param refresh - delete the existing entity map before running dedupe.

    :return:
    """

    # Load settings and configure dedupe object
    if not os.path.exists(settings_file):
        raise Exception(f"No settings file found, searched for {settings_file}")

    with open(settings_file, "rb") as sf:
        deduper = dedupe.StaticDedupe(sf, num_cores=4)

    engine = get_engine(refresh=True)
    conn = engine.connect()
    # Create blocking map table
    metadata_obj = MetaData()
    blocking_map_table = Table(
        "dedupe_blocking_map",
        DedupeBlockingMap.metadata,
    )

    # Clear blocking map table.
    conn.execute(delete(blocking_map_table))
    conn.commit()
    conn.close()

    engine = get_engine(refresh=True, yield_per=1000)
    conn = engine.connect()

    # Create inverted index
    employer_record_table = get_employer_record_table(engine)
    for field in deduper.fingerprinter.index_fields:
        field_data = conn.execute(
            select(getattr(employer_record_table.c, field)).distinct()
        ).scalars()
        deduper.fingerprinter.index(field_data, field)

    # Write blocking map
    print("Writing blocking map")
    employers = conn.execute(
        select(employer_record_table).order_by(employer_record_table.c.id)
    )
    full_data = ((row._mapping["id"], row._mapping) for row in employers)  # noqa
    blocking_data = [
        {"block_key": v1, "employer_record_id": v2}
        for (v1, v2) in deduper.fingerprinter(full_data)
    ]

    conn.close()

    engine = get_engine(refresh=True)
    conn = engine.connect()

    # TODO: The example online uses a bulk CSV insert here to speed up things. Is this necessary?
    conn.execute(insert(blocking_map_table), blocking_data)
    conn.commit()

    # This just frees up memory
    deduper.fingerprinter.reset_indices()

    # Create entity map table.
    entity_map_table = get_cluster_table(engine)
    if refresh:
        conn.execute(delete(entity_map_table))
        conn.commit()

    metadata_obj.create_all(
        engine,
        tables=[
            entity_map_table,
        ],
    )

    # Actually do clustering!
    print("Starting clustering")
    blocking_map_left = blocking_map_table.alias()
    blocking_map_right = blocking_map_table.alias()
    blocking_map_subquery = (
        select(
            blocking_map_left.c.employer_record_id.label("left_id"),
            blocking_map_right.c.employer_record_id.label("right_id"),
        )
        .join_from(
            blocking_map_left,
            blocking_map_right,
            blocking_map_left.c.block_key == blocking_map_right.c.block_key,
        )
        .where(
            blocking_map_left.c.employer_record_id
            < blocking_map_right.c.employer_record_id
        )
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
        deduper.score(employer_record_pairs(clusters_records)),
        threshold=DEDUPE_CLUSTERING_THRESHOLD,
    )
    print("Finished clustering, starting writing results")

    # Write out results
    [  # noqa
        conn.execute(insert(entity_map_table), i) for i in cluster_ids(clustered_dupes)
    ]
    conn.commit()
    print("Finished writing results")
    conn.close()
    return True


if __name__ == "__main__":
    build_cluster_table(refresh=True)
