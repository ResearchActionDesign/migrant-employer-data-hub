import os
from datetime import datetime

from prettytable import PrettyTable
from sqlalchemy import func, null, select, update

from app.actions.dedupe import get_cluster_table, get_employer_record_table
from app.db import get_engine
from app.models.dol_disclosure_job_order import DolDisclosureJobOrder  # noqa
from app.settings import DEDUPE_CLUSTER_REVIEW_THRESHOLD


def review_clusters(limit=10) -> None:
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
            func.group_concat(cluster_table.c.employer_record_id, ",").label(
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
        os.system("clear")

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
        rows.sort(key=lambda r: r[1])
        table_printer.add_rows(rows)
        print("Cluster members:")
        print(table_printer)

        if n % 5 == 0:
            print(f"You have reviewed {n} of {total_to_review} so far")
            conn.commit()

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

    conn.commit()
    conn.close()


if __name__ == "__main__":
    review_clusters()
