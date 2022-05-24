"""Provides functionality to generate admission event logs for a given cohort"""
import logging
import pandas as pd
from psycopg2.extensions import cursor
from .extraction_helper import (
    get_filename_string, extract_admissions_for_admission_ids)


logger = logging.getLogger('cli')


def extract_admission_events(db_cursor: cursor, cohort: pd.DataFrame,
                             save_intermediate: bool) -> pd.DataFrame:
    """
    Extracts admission events for a given cohort
    """

    logger.info("Begin extracting admission events!")

    hospital_admission_ids = list(cohort["hadm_id"].unique())
    hospital_admission_ids = [float(i) for i in hospital_admission_ids]
    admission_ids = hospital_admission_ids

    admissions = extract_admissions_for_admission_ids(db_cursor, admission_ids)

    event_dict = {}
    i = 0
    for _, row in admissions.iterrows():
        if pd.isnull(row["deathtime"]):
            column_labels = ["admittime",
                             "dischtime", "edregtime", "edouttime"]
        else:
            column_labels = ["admittime",
                             "deathtime", "edregtime", "edouttime"]
        for col in admissions.columns:
            if col in column_labels:
                if pd.isna(row[col]):
                    continue
                activity = col.replace('time', '')
                new_row = {"hadm_id": row["hadm_id"], "subject_id": row["subject_id"],
                           "activity": activity, "timestamp": row[col]}
                event_dict[i] = new_row
                i = i + 1

    log = pd.DataFrame.from_dict(event_dict, "index")  # type: ignore
    log = log.sort_values("timestamp")
    log = log.reset_index().drop("index", axis=1)
    log = log.rename({"activity": "concept:name",
                     "timestamp": "time:timestamp"}, axis=1)

    if save_intermediate:
        filename = get_filename_string("admission_log", ".csv")
        log.to_csv("output/" + filename)

    logger.info("Done extracting admission events!")

    return log
