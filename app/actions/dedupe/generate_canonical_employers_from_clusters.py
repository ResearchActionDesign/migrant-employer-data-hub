from typing import Union

import rollbar
from affinegap import normalizedAffineGapDistance
from sqlalchemy import Integer, String, exc, func, or_, select, true, update
from sqlmodel import Session
from sqlmodel import select as sqlmodel_select

from app.actions.dedupe import get_cluster_table, get_employer_record_table
from app.db import get_engine
from app.models.dol_disclosure_job_order import DolDisclosureJobOrder  # noqa
from app.models.unique_employer import UniqueEmployer
from app.settings import DB_ENGINE, DEDUPE_CLUSTER_REVIEW_THRESHOLD, ROWS_BEFORE_COMMIT


def canonicalize(
    record_cluster: list[dict[str, Union[str, None]]]
) -> dict[str, Union[str, None]]:
    """
    Rewrite of dedupe.canonicalize to save memory by not relying on Numpy
    :param record_cluster:
    :return:
    """

    if len(record_cluster) == 1:
        return record_cluster[0]

    keys = record_cluster[0].keys()
    canonical_rep = {}

    for key in keys:
        canonical_rep[key] = ""

        record_values = [r[key] for r in record_cluster if r.get(key)]
        if len(record_values) == 1:
            canonical_rep[key] = record_values[0]
            continue

        current_min_dist = None
        distances: dict[int, dict[int, float]] = {}
        for i, _ in enumerate(record_values):
            for j in range(0, i):
                if not distances.get(i):
                    distances[i] = {}
                if not distances.get(j):
                    distances[j] = {}
                distances[i][j] = distances[j][i] = normalizedAffineGapDistance(
                    record_values[i], record_values[j]
                )

        current_min_dist = None
        current_canonical_value = None
        for i, _ in enumerate(record_values):
            dist = None
            if i in distances:
                dist = sum(distances[i].values())
            if dist and (not current_min_dist or dist < current_min_dist):
                current_min_dist = dist
                current_canonical_value = record_values[i]

        canonical_rep[key] = current_canonical_value

    return canonical_rep


def process_single_cluster(cluster, cluster_table, engine, conn) -> None:
    session = Session(engine)
    if isinstance(cluster.employer_record_ids, str):
        cluster_employer_record_ids = cluster.employer_record_ids.split(",")
    else:
        cluster_employer_record_ids = cluster.employer_record_ids

    employer_record_table = get_employer_record_table(engine)
    employer_records = conn.execute(
        select(employer_record_table).where(
            employer_record_table.c.id.in_(cluster_employer_record_ids)
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

    canonical_record = canonicalize(employer_records_dicts)

    # If for some reason we should have an existing unique employer matching this, check first!
    canonical_employer = session.exec(
        sqlmodel_select(UniqueEmployer).where(
            UniqueEmployer.name == canonical_record["name"],
            UniqueEmployer.trade_name_dba
            == canonical_record.get("trade_name_dba", None),
            UniqueEmployer.city == canonical_record.get("city", None),
            UniqueEmployer.state == canonical_record.get("state", None),
            UniqueEmployer.country == canonical_record.get("country", None),
            UniqueEmployer.phone == canonical_record.get("phone", None),
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
        .where(employer_record_table.c.id.in_(cluster_employer_record_ids))
        .values(unique_employer_id=canonical_employer.id)
    )
    conn.execute(
        update(cluster_table)
        .where(cluster_table.c.employer_record_id.in_(cluster_employer_record_ids))
        .values(processed_to_canonical_employer=True)
    )
    print(f"Generated employer: {canonical_employer}")
    conn.commit()
    session.close()


def generate_canonical_employers_from_clusters(
    batch_limit: int = ROWS_BEFORE_COMMIT,
) -> None:
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
    if DB_ENGINE == "postgres":
        cluster_table_subquery = (
            select(
                cluster_table.c.canon_id,
                func.array_agg(cluster_table.c.employer_record_id).label(
                    "employer_record_ids"
                ),
                # Sometimes a new row in the cluster will get reviewed and needed to be added to cluster, so
                # need to check processed status across the cluster.
                func.max(
                    cluster_table.c.processed_to_canonical_employer.cast(Integer)
                ).label("already_processed_max"),
                func.min(
                    cluster_table.c.processed_to_canonical_employer.cast(Integer)
                ).label("already_processed_min"),
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
    else:
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
        .where(cluster_table_subquery.c.already_processed_min == 0)
        .limit(batch_limit)
    )

    valid_clusters = list(conn.execute(cluster_table_query))

    for cluster in valid_clusters:
        with conn.begin_nested():
            if cluster.already_processed_max and cluster.already_processed_min:
                # In this case, all rows in the cluster have been processed, we can continue.
                continue

            if not cluster.already_processed_min and not cluster.already_processed_max:
                process_single_cluster(cluster, cluster_table, engine, conn)

            else:
                if isinstance(cluster.employer_record_ids, str):
                    cluster_employer_record_ids = cluster.employer_record_ids.split(",")
                else:
                    cluster_employer_record_ids = cluster.employer_record_ids

                # In this last case, we just need to set canonical employer ID for the remaining rows in the cluster.
                employer_record_table = get_employer_record_table(engine)
                employer_uuid = (
                    conn.execute(
                        select(
                            func.max(
                                employer_record_table.c.unique_employer_id.cast(String)
                            )
                        ).where(
                            employer_record_table.c.id.in_(cluster_employer_record_ids)
                        )
                    )
                    .scalars()
                    .first()
                )
                conn.execute(
                    update(employer_record_table)
                    .values(unique_employer_id=employer_uuid)
                    .where(employer_record_table.c.id.in_(cluster_employer_record_ids))
                )
                conn.execute(
                    update(cluster_table)
                    .values(processed_to_canonical_employer=True)
                    .where(
                        cluster_table.c.employer_record_id.in_(
                            cluster_employer_record_ids
                        )
                    )
                )
                try:
                    conn.commit()
                except exc.DBAPIError as e:
                    rollbar.report_message(
                        e,
                        "error",
                    )

    conn.close()


if __name__ == "__main__":
    generate_canonical_employers_from_clusters(5)
