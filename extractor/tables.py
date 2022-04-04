"""Provides functionality to retrieve events from a list of tables"""
import logging
import pandas as pd
from .helper import (get_filename_string, extract_table_for_admission_ids, extract_table,
                     extract_emergency_department_stays_for_admission_ids,
                     extract_ed_table_for_ed_stays,
                     core_tables, hosp_tables, icu_tables, ed_tables, detail_tables,
                     detail_foreign_keys)


logger = logging.getLogger('cli')


def extract_table_events(db_cursor, cohort, table_list) -> pd.DataFrame:
    """
    Extracts events from a given list of tables for a given cohort
    """

    logger.info("Begin extracting events from provided tables!")

    hospital_admission_ids = list(cohort["hadm_id"].unique())
    hospital_admission_ids = [float(i) for i in hospital_admission_ids]

    final_log = pd.DataFrame()

    for table in table_list:
        if table in core_tables:
            module = "mimic_core"
        elif table in hosp_tables:
            module = "mimic_hosp"
        elif table in icu_tables:
            module = "mimic_icu"
        elif table in ed_tables:
            module = "mimic_ed"

        if module == "mimic_ed":
            ed_stays = extract_emergency_department_stays_for_admission_ids(
                       db_cursor, hospital_admission_ids)
            ed_stays = ed_stays[["subject_id", "hadm_id", "stay_id"]]
            ed_stay_list = list(ed_stays["stay_id"])
            table_content = extract_ed_table_for_ed_stays(db_cursor, ed_stay_list, table)
            table_content = table_content.merge(ed_stays, on=["stay_id", "subject_id"], how="inner")
        else:
            table_content = extract_table_for_admission_ids(
                            db_cursor, hospital_admission_ids, module, table)

        try:
            detail_table = detail_tables[table]
        except KeyError:
            detail_table = None

        if detail_table is not None:
            detail_content = extract_table(db_cursor, module, detail_table)
            detail_foreign_key = detail_foreign_keys[detail_table]
            print(detail_foreign_key)
            table_content = table_content.merge(detail_content, on=detail_foreign_key, how="left")

        columns = list(table_content.columns)
        time_columns = list(filter(lambda col: "time" in col or "date" in col, columns))
        ativity_columns = list(filter(lambda col: "time" not in col and "date" not in col
                                                    and "id" not in col, columns))

        logger.info("The table %s includes the following activity columns: ", table)
        logger.info('[%s]' % ', '.join(map(str, ativity_columns)))
        chosen_activity_column = input("Choose the activity column:")

        if len(time_columns) == 1:
            chosen_time_column = time_columns[0]
        else:
            logger.info("The table %s includes multiple timestamps: ", table)
            logger.info('[%s]' % ', '.join(map(str, time_columns)))
            chosen_time_column = input("Choose the timestamp column:")

        table_content = table_content.rename(columns={chosen_time_column:"time:timestamp",
                                             chosen_activity_column:"concept:name"})

        final_log = pd.concat([final_log, table_content])




    filename = get_filename_string("table_log", ".csv")
    final_log.to_csv("output/" + filename)
    logger.info("Done extracting events from provided tables!")
    return final_log
