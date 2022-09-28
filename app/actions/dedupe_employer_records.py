"""
Dedupe records using dedupe package.

This part of the code uses raw sqlalchemy rather than SQLModel in order to be able to avoid some of the overhead required.
@see https://dedupeio.github.io/dedupe-examples/docs/pgsql_big_dedupe_example.html
"""

import os
from datetime import datetime
from typing import Generator, Iterable, Tuple, Union

import dedupe
from prettytable import PrettyTable
from sqlalchemy import (
    MetaData,
    Table,
    false,
    func,
    insert,
    null,
    or_,
    select,
    true,
    update,
)
from sqlmodel import Session
from sqlmodel import select as sqlmodel_select

from app.db import get_engine
from app.models.dedupe_blocking_map import DedupeBlockingMap
from app.models.dedupe_entity_map import DedupeEntityMap
from app.models.dol_disclosure_job_order import DolDisclosureJobOrder  # noqa
from app.models.employer_record import EmployerRecord
from app.models.unique_employer import UniqueEmployer
from app.settings import (
    DEDUPE_CLUSTER_REVIEW_THRESHOLD,
    DEDUPE_CLUSTERING_THRESHOLD,
    ROWS_BEFORE_COMMIT,
    TRAINING_RECALL_PERCENT,
    TRAINING_SAMPLE_SIZE,
)

# TODO: Load these from somewhere stable -- S3?
settings_file = "pgsql_big_dedupe_example_settings"
training_file = "pgsql_big_dedupe_example_training.json"


def employer_record_pairs(
    result_set: Iterable,
) -> Generator[Tuple[Tuple[int, dict], Tuple[int, dict]], None, None]:
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


def interactively_train_model() -> None:
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

    employer_record_table = get_employer_record_table(engine)
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

    if os.path.exists(training_file):
        print("reading labeled examples from ", training_file)
        with open(training_file, "rt") as tf:
            deduper.prepare_training(data_set, tf, sample_size=TRAINING_SAMPLE_SIZE)
    else:
        deduper.prepare_training(data_set, sample_size=TRAINING_SAMPLE_SIZE)

    del data_set

    dedupe.console_label(deduper)

    with open(training_file, "wt") as tf:
        deduper.write_training(tf)

    deduper.train(recall=TRAINING_RECALL_PERCENT)
    with open(settings_file, "wb") as sf:
        deduper.write_settings(sf)
    deduper.cleanup_training()
    conn.close()


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

    engine = get_engine(refresh=True, yield_per=1000)
    conn = engine.connect()

    # Create blocking map table
    metadata_obj = MetaData()
    blocking_map_table = Table(
        "dedupe_blocking_map",
        DedupeBlockingMap.metadata,
    )
    DedupeBlockingMap.metadata.drop_all(engine, tables=[blocking_map_table])
    DedupeBlockingMap.metadata.create_all(engine, tables=[blocking_map_table])

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

    # TODO: The example online uses a bulk CSV insert here to speed up things. Is this necessary?
    conn.execute(insert(blocking_map_table), blocking_data)
    conn.commit()

    # This just frees up memory
    deduper.fingerprinter.reset_indices()

    # Create entity map table.
    entity_map_table = get_cluster_table(engine)
    if refresh:
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


def review_clusters(limit=5) -> None:
    """
    Interactively review low-confidence clusters to mark if they should be included or not.
    :return:
    """
    engine = get_engine()
    conn = engine.connect()

    cluster_table = get_cluster_table(engine)
    employer_record_table = get_employer_record_table(engine)

    cluster_table_subquery = (
        select(
            cluster_table.c.canon_id,
            func.group_concat(cluster_table.c.employer_record_id).label(
                "employer_record_ids"
            ),
        )
        .group_by(cluster_table.c.canon_id)
        .subquery()
    )

    total_to_review = (
        conn.execute(
            select(func.count(cluster_table.c.employer_record_id).label("count"))
            .where(cluster_table.c.cluster_score <= DEDUPE_CLUSTER_REVIEW_THRESHOLD)
            .where(cluster_table.c.review_date == null())
        )
        .first()
        .count
    )

    cluster_inclusions_to_review = conn.execute(
        select(cluster_table, cluster_table_subquery)
        .where(cluster_table.c.cluster_score <= DEDUPE_CLUSTER_REVIEW_THRESHOLD)
        .where(cluster_table.c.review_date == null())
        .join(
            cluster_table_subquery,
            cluster_table.c.canon_id == cluster_table_subquery.c.canon_id,
        )
        .limit(limit)
    )
    for n, cluster in enumerate(cluster_inclusions_to_review):
        # Load all employer nodes for that cluster.
        employers_in_cluster = conn.execute(
            select(employer_record_table).where(
                employer_record_table.c.id.in_(cluster.employer_record_ids.split(","))
            )
        )
        table_printer = PrettyTable()
        table_printer.align = "l"
        table_printer.field_names = [
            "*",
            "ID",
            "Name",
            "Trade Name",
            "City",
            "State",
            "Country",
            "Phone",
        ]
        rows = [
            (
                "*" if e.id == cluster.employer_record_id else "",
                e.id,
                e.name,
                e.trade_name_dba,
                e.city,
                e.state,
                e.country,
                e.phone,
            )
            for e in employers_in_cluster
        ]
        rows.sort(key=lambda r: r[0])
        table_printer.add_rows(rows)
        print("Cluster members:")
        print(table_printer)

        prompt = (
            "Does the starred row belong in the cluster?\n(y)es (n)o (u)nsure    (q)uit"
        )
        print(prompt)
        i = input()
        valid_responses = ["y", "n", "u", "q"]

        while i not in valid_responses:
            print(prompt)
            i = input()

        timestamp = datetime.now()

        if i == "q":
            break

        is_valid_cluster = None
        if i == "y":
            is_valid_cluster = True

        if i == "n":
            is_valid_cluster = False
        conn.execute(
            update(cluster_table)
            .where(cluster_table.c.employer_record_id == cluster.employer_record_id)
            .values(review_date=timestamp, is_valid_cluster=is_valid_cluster)
        )

        if n % 10 == 0:
            print(f"You have reviewed {n} of {total_to_review} so far")
            conn.commit()

    conn.commit()
    conn.close()


def process_single_cluster(cluster, cluster_table, engine, conn) -> None:
    session = Session(engine)

    employer_record_table = get_employer_record_table(engine)
    employer_records = conn.execute(
        select(employer_record_table).where(
            employer_record_table.c.id.in_(cluster.employer_record_ids.split(","))
        )
    )

    employer_records_dicts = []
    last_seen = None
    first_seen = None
    sources = []
    for e in employer_records:
        employer_records_dicts.append(
            {
                "name": e.name,
                "trade_name_dba": e.trade_name_dba,
                "city": e.city,
                "state": e.state,
                "country": e.country,
                "phone": e.phone,
            }
        )

        if not last_seen or e.last_seen > last_seen:
            last_seen = e.last_seen
        if not first_seen or e.first_seen < first_seen:
            first_seen = e.first_seen
        if e.source not in sources:
            sources.append(e.source)

    canonical_record = dedupe.canonicalize(employer_records_dicts)

    # If for some reason we should have an existing unique employer matching this, check first!
    canonical_employer = session.exec(
        sqlmodel_select(UniqueEmployer).where(
            UniqueEmployer.name == canonical_record["name"],
            UniqueEmployer.trade_name_dba == canonical_record["trade_name_dba"],
            UniqueEmployer.city == canonical_record["city"],
            UniqueEmployer.state == canonical_record["state"],
            UniqueEmployer.country == canonical_record["country"],
            UniqueEmployer.phone == canonical_record["phone"],
        )
    ).all()
    if len(canonical_employer) == 1:
        canonical_employer = canonical_employer[0]
    elif len(canonical_employer) == 0:
        canonical_employer = UniqueEmployer(
            first_seen=first_seen,
            last_seen=last_seen,
            sources=sources,
            **canonical_record,
        )
        session.add(canonical_employer)
        session.commit()
        session.refresh(canonical_employer)
    else:
        raise Exception(
            f"Found more than one matching canonical employer for {canonical_record}"
        )

    conn.execute(
        update(employer_record_table)
        .where(employer_record_table.c.id.in_(cluster.employer_record_ids.split(",")))
        .values(unique_employer_id=canonical_employer.id)
    )
    conn.execute(
        update(cluster_table)
        .where(
            cluster_table.c.employer_record_id.in_(
                cluster.employer_record_ids.split(",")
            )
        )
        .values(processed_to_canonical_employer=True)
    )
    print(f"Generated employer: {canonical_employer}")
    conn.commit()
    session.close()


def generate_canonical_employers_from_non_clustered_records() -> None:
    engine = get_engine()
    session = Session(engine)

    employers_with_no_cluster = session.exec(
        sqlmodel_select(EmployerRecord)
        .join(
            DedupeEntityMap,
            DedupeEntityMap.employer_record_id == EmployerRecord.id,
            isouter=True,
        )
        .where(DedupeEntityMap.employer_record_id == null())
        .where(EmployerRecord.unique_employer_id == null())
    )

    i = 0
    for e in employers_with_no_cluster:
        unique_employer = UniqueEmployer(
            name=e.name,
            trade_name_dba=e.trade_name_dba,
            city=e.city,
            state=e.state,
            country=e.country,
            phone=e.phone,
            last_seen=e.last_seen,
            first_seen=e.first_seen,
            sources=[
                e.source,
            ],
        )
        e.unique_employer = unique_employer
        session.add(unique_employer)
        session.add(e)
        i += 1

        if i % ROWS_BEFORE_COMMIT == 0:
            print(f"Added {i} new employers from non-clustered records.")
            session.commit()

    if i > 0:
        print(f"Added {i} new employers from non-clustered records.")
        session.commit()
    session.close()


def generate_canonical_employers_from_clusters(batch_limit: int = 500) -> None:
    """
    Iterate through all clusters with confidence > 0.8 or those which
    have been manually reviewed. For each group of nodes, generate a canonical
    employer record; then link it to the relevant existing employer records.

    :param batch_limit:
    :return:
    """

    # Load all valid clusters
    engine = get_engine()
    conn = engine.connect()

    cluster_table = get_cluster_table(engine)
    cluster_table_subquery = (
        select(
            cluster_table.c.canon_id,
            func.group_concat(cluster_table.c.employer_record_id).label(
                "employer_record_ids"
            ),
            # Sometimes a new row in the cluster will get reviewed and needed to be added to cluster, so
            # need to check processed status across the cluster.
            func.max(cluster_table.c.processed_to_canonical_employer).label(
                "already_processed_max"
            ),
            func.min(cluster_table.c.processed_to_canonical_employer).label(
                "already_processed_min"
            ),
        )
        .where(
            or_(
                cluster_table.c.cluster_score > DEDUPE_CLUSTER_REVIEW_THRESHOLD,
                cluster_table.c.is_valid_cluster == true(),
            )
        )
        .group_by(cluster_table.c.canon_id)
        .subquery()
    )
    cluster_table_query = (
        select(cluster_table_subquery)
        .where(
            or_(
                cluster_table_subquery.c.already_processed_max == false(),
                cluster_table_subquery.c.already_processed_min == false(),
            )
        )
        .limit(batch_limit)
    )

    valid_clusters = list(conn.execute(cluster_table_query))

    for cluster in valid_clusters:
        if cluster.already_processed_max and cluster.already_processed_min:
            # In this case, all rows in the cluster have been processed, we can continue.
            continue

        if not cluster.already_processed_min and not cluster.already_processed_max:
            process_single_cluster(cluster, cluster_table, engine, conn)

        else:
            # In this last case, we just need to set canonical employer ID for the remaining rows in the cluster.
            employer_record_table = get_employer_record_table(engine)
            employer_uuid = (
                conn.execute(
                    select(func.max(employer_record_table.c.unique_employer_id)).where(
                        employer_record_table.c.id.in_(
                            cluster.employer_record_ids.split(",")
                        )
                    )
                )
                .scalars()
                .first()
            )
            conn.execute(
                update(employer_record_table)
                .values(unique_employer_id=employer_uuid)
                .where(
                    employer_record_table.c.id.in_(
                        cluster.employer_record_ids.split(",")
                    )
                )
            )
            conn.execute(
                update(cluster_table)
                .values(processed_to_canonical_employer=True)
                .where(
                    cluster_table.c.employer_record_id.in_(
                        cluster.employer_record_ids.split(",")
                    )
                )
            )
            conn.commit()

    conn.close()


if __name__ == "__main__":
    # interactively_train_model()
    # build_cluster_table()
    # review_clusters()
    # generate_canonical_employers_from_non_clustered_records()
    generate_canonical_employers_from_clusters()
