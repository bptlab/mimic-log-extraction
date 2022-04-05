"""Provides functionality to retrieve events from a list of tables"""
import logging
import pandas as pd
from typing import List
from .helper import (extract_table_columns, get_filename_string, extract_table_for_admission_ids,
                     extract_table, extract_emergency_department_stays_for_admission_ids,
                     extract_ed_table_for_ed_stays,
                     core_tables, hosp_tables, icu_tables, ed_tables, detail_tables,
                     detail_foreign_keys)


logger = logging.getLogger('cli')


def extract_table_events(db_cursor, cohort: pd.DataFrame, table_list: List[str]) -> pd.DataFrame:
    """
    Extracts events from a given list of tables for a given cohort
    """

    logger.info("Begin extracting events from provided tables!")

    hospital_admission_ids = list(cohort["hadm_id"].unique())
    hospital_admission_ids = [float(i) for i in hospital_admission_ids]

    chosen_activity_time = ask_activity_and_time(db_cursor, table_list)

    logger.info("Extracting events from provided tables. This may take a while...")

    final_log = extract_tables(db_cursor, table_list, hospital_admission_ids, chosen_activity_time)
    final_log = final_log.sort_values(["hadm_id", "time:timestamp"])
    filename = get_filename_string("table_log", ".csv")
    final_log.to_csv("output/" + filename)

    logger.info("Done extracting events from provided tables!")

    return final_log

def ask_activity_and_time(db_cursor, table_list: List[str]) -> dict:
    """
    Derives columns from tables and asks for activity and timestamp column
    """
    chosen_activity_time = {}
    for table in table_list:
        if table in core_tables:
            module = "mimic_core"
        elif table in hosp_tables:
            module = "mimic_hosp"
        elif table in icu_tables:
            module = "mimic_icu"
        elif table in ed_tables:
            module = "mimic_ed"

        table_columns = extract_table_columns(db_cursor, module, table)

        try:
            detail_table = detail_tables[table]
        except KeyError:
            detail_table = None
        if detail_table is not None:
            detail_columns = extract_table_columns(db_cursor, module, detail_table)

        table_columns = table_columns + detail_columns

        time_columns = list(filter(lambda col: "time" in col or "date" in col, table_columns))
        ativity_columns = list(filter(lambda col: "time" not in col and "date" not in col
                                                        and "id" not in col, table_columns))

        logger.info("The table %s includes the following activity columns: ", table)
        logger.info('[%s]' % ', '.join(map(str, ativity_columns)))
        chosen_activity_column = input("Choose the activity column:")

        if len(time_columns) == 1:
            chosen_time_column = time_columns[0]
        else:
            logger.info("The table %s includes multiple timestamps: ", table)
            logger.info('[%s]' % ', '.join(map(str, time_columns)))
            chosen_time_column = input("Choose the timestamp column:")
        chosen_activity_time[table] = [chosen_activity_column, chosen_time_column]

    return chosen_activity_time

def extract_tables(db_cursor, table_list, hospital_admission_ids,
                   chosen_activity_time) -> pd.DataFrame:
    """
    Extracts given tables from the database and generates an event log
    """

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
            table_content = table_content.merge(detail_content, on=detail_foreign_key, how="left")


        table_content = table_content.rename(columns={
                                                chosen_activity_time[table][1]:"time:timestamp",
                                                chosen_activity_time[table][0]:"concept:name"})

        final_log = pd.concat([final_log, table_content])

    return final_log
