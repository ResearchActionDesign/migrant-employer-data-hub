import os
import time

import dedupe
from sqlalchemy import text

from app.actions.dedupe import settings_file, training_file
from app.db import get_engine
from app.models.dol_disclosure_job_order import DolDisclosureJobOrder  # noqa
from app.settings import TRAINING_RECALL_PERCENT, TRAINING_SAMPLE_SIZE


def train_dedupe_model() -> None:
    fields = [
        {"field": "name", "type": "Name"},
        {"field": "trade_name_dba", "type": "Name", "has_missing": True},
        {"field": "city", "type": "Exact"},
        {"field": "state", "type": "Exact"},
        {"field": "country", "type": "Exact", "has_missing": True},
        {"field": "phone", "type": "Exact"},
    ]
    deduper = dedupe.Dedupe(fields, num_cores=4)

    t = time.time()
    # Yield_per execution option forces use of a server-side cursor, 1,000 is the number of results to buffer in memory.
    engine = get_engine(yield_per=1000)
    conn = engine.connect()

    # Load results.
    employers = conn.execute(
        text(
            f"""
    select id, name, trade_name_dba, city, state, country, phone
    from employer_record order by name
    limit {4*TRAINING_SAMPLE_SIZE}"""  # TODO: Remove this limit?
        )
    )
    print(f"Select query: {time.time() - t}")
    t = time.time()

    data_set = {
        # Need to cast the row objects to a dictionary for dedupe to recognize them, annoyingly!
        i: row._asdict()
        for i, row in enumerate(employers)
    }
    print(f"Generate dict: {time.time() - t}")
    t = time.time()

    if os.path.exists(training_file):
        print("reading labeled examples from ", training_file)
        with open(training_file, "rt") as tf:
            deduper.prepare_training(data_set, tf, sample_size=TRAINING_SAMPLE_SIZE)
    else:
        deduper.prepare_training(data_set, sample_size=TRAINING_SAMPLE_SIZE)

    del employers
    print(f"Prepare training: {time.time() - t}")
    t = time.time()

    dedupe.console_label(deduper)

    with open(training_file, "wt") as tf:
        deduper.write_training(tf)

    deduper.train(recall=TRAINING_RECALL_PERCENT)
    with open(settings_file, "wb") as sf:
        deduper.write_settings(sf)
    deduper.cleanup_training()
    conn.close()


if __name__ == "__main__":
    train_dedupe_model()
