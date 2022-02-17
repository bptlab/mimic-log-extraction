from psycopg2 import connect
import pandas as pd

from .helper import (get_filename_string, extract_admissions_for_hadms,
                     extract_ed_stays_for_hadms, extract_triage_stays_for_ed_stays)


def extract_admission_events(db_cursor, cohort):

    print("Begin extracting admission events!")

    hadm_list = list(cohort["hadm_id"].unique())
    hadm_list = [float(i) for i in hadm_list]
    hadms = hadm_list

    adm = extract_admissions_for_hadms(db_cursor, hadms)

    ed_stay = extract_ed_stays_for_hadms(db_cursor, hadms)

    ed_stay = ed_stay[["subject_id", "hadm_id", "stay_id"]]
    ed_stays = list(ed_stay["stay_id"])

    triage = extract_triage_stays_for_ed_stays(db_cursor, ed_stays)

    triage = triage.drop("subject_id", axis=1)

    ed_reg_info = ed_stay.merge(triage, on="stay_id", how="left")

    ed_reg_config = ['temperature', 'heartrate', 'resprate',
                     'o2sat', 'sbp', 'dbp', 'pain', 'acuity', 'chiefcomplaint']

    adm_config = ['admission_location', 'insurance',
                  'language', 'marital_status', 'ethnicity']

    log = pd.DataFrame()
    d = {}
    i = 0
    for _, row in adm.iterrows():
        ed_reg_row = ed_reg_info.loc[ed_reg_info["hadm_id"] == row["hadm_id"]]
        if(pd.isnull(row["deathtime"])):
            l = ["admittime", "dischtime", "edregtime", "edouttime"]
        else:
            l = ["admittime", "deathtime", "edregtime", "edouttime"]
        for col in adm.columns:
            if(col in l):
                activity = col.replace('time', '')
                new_row = {"case_id": row["subject_id"],
                           "activity": activity, "timestamp": row[col]}
                if (col == "admittime"):
                    for e_at in adm_config:
                        new_row[e_at] = row[e_at]
                if ((col == "edregtime") & (len(ed_reg_row) > 0)):
                    for e_at in ed_reg_config:
                        new_row[e_at] = ed_reg_row[e_at].iloc[0]
                d[i] = new_row
                i = i + 1
    log = pd.DataFrame.from_dict(d, "index")
    log = log.sort_values("timestamp")

    filename = get_filename_string("admission_log", ".csv")
    log.to_csv("output/" + filename)

    print("Done extracting admission events!")

    return log
