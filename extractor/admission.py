"""Provides functionality to generate admission event logs for a given cohort"""
import logging
import pandas as pd
from .helper import (get_filename_string, extract_admissions_for_admission_ids,
                     extract_emergency_department_stays_for_admission_ids,
                     extract_triage_stays_for_ed_stays)


logger = logging.getLogger('cli')


def extract_admission_events(db_cursor, cohort) -> pd.DataFrame:
    """
    Extracts transfer events for a given cohort
    """

    logger.info("Begin extracting admission events!")

    hospital_admission_ids = list(cohort["hadm_id"].unique())
    hospital_admission_ids = [float(i) for i in hospital_admission_ids]
    admission_ids = hospital_admission_ids

    admissions = extract_admissions_for_admission_ids(db_cursor, admission_ids)

    ed_stay = extract_emergency_department_stays_for_admission_ids(
        db_cursor, admission_ids)

    ed_stay = ed_stay[["subject_id", "hadm_id", "stay_id"]]
    ed_stay_ids = list(ed_stay["stay_id"])

    triage_stays = extract_triage_stays_for_ed_stays(db_cursor, ed_stay_ids)

    triage_stays = triage_stays.drop("subject_id", axis=1)

    ed_reg_info = ed_stay.merge(triage_stays, on="stay_id", how="left")

    ed_reg_config = ['temperature', 'heartrate', 'resprate',
                     'o2sat', 'sbp', 'dbp', 'pain', 'acuity', 'chiefcomplaint']

    admission_config = ['admission_location', 'insurance',
                        'language', 'marital_status', 'ethnicity']

    log = pd.DataFrame()
    event_dict = {}
    i = 0
    for _, row in admissions.iterrows():
        ed_reg_row = ed_reg_info.loc[ed_reg_info["hadm_id"] == row["hadm_id"]]
        if pd.isnull(row["deathtime"]):
            column_labels = ["admittime",
                             "dischtime", "edregtime", "edouttime"]
        else:
            column_labels = ["admittime",
                             "deathtime", "edregtime", "edouttime"]
        for col in admissions.columns:
            if col in column_labels:
                activity = col.replace('time', '')
                new_row = {"case_id": row["subject_id"],
                           "activity": activity, "timestamp": row[col]}
                if col == "admittime":
                    for e_at in admission_config:
                        new_row[e_at] = row[e_at]
                if (col == "edregtime") & (len(ed_reg_row) > 0):
                    for e_at in ed_reg_config:
                        new_row[e_at] = ed_reg_row[e_at].iloc[0]
                event_dict[i] = new_row
                i = i + 1
    log = pd.DataFrame.from_dict(event_dict, "index")  # type: ignore
    log = log.sort_values("timestamp")

    filename = get_filename_string("admission_log", ".csv")
    log.to_csv("output/" + filename)

    logger.info("Done extracting admission events!")

    return log
