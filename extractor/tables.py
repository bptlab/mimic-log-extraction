"""Provides functionality to retrieve events from a list of tables"""
import logging
from typing import List, Optional
import pandas as pd
from psycopg2.extensions import cursor
from extractor.admission import extract_admission_events
from .extraction_helper import (extract_table_columns, get_filename_string,
                                extract_table_for_admission_ids, extract_table,
                                extract_emergency_department_stays_for_admission_ids,
                                extract_ed_table_for_ed_stays, get_table_module,
                                extract_icustay_events, detail_tables, detail_foreign_keys)



logger = logging.getLogger('cli')


def extract_table_events(db_cursor: cursor, cohort: pd.DataFrame, table_list: List[str],
                         tables_activities: Optional[List[str]],
                         tables_timestamps: Optional[List[str]],
                         save_intermediate: bool) -> pd.DataFrame:
    """
    Extracts events from a given list of tables for a given cohort
    """

    logger.info("Begin extracting events from provided tables!")

    hospital_admission_ids = list(cohort["hadm_id"].unique())
    hospital_admission_ids = [float(i) for i in hospital_admission_ids]

    chosen_activity_time = ask_activity_and_time(db_cursor, table_list, tables_activities,
                                                 tables_timestamps)

    logger.info(
        "Extracting events from provided tables. This may take a while...")

    final_log = extract_tables(
        db_cursor, table_list, hospital_admission_ids, chosen_activity_time, cohort)
    final_log = final_log.sort_values(["hadm_id", "time:timestamp"])
    if save_intermediate:
        filename = get_filename_string("table_log", ".csv")
        final_log.to_csv("output/" + filename)

    logger.info("Done extracting events from provided tables!")

    return final_log


def ask_activity_and_time(db_cursor: cursor, table_list: List[str],
                          tables_activities: Optional[List[str]],
                          tables_timestamps: Optional[List[str]]) -> dict:
    """
    Derives columns from tables and asks for activity and timestamp column
    """
    chosen_activity_time = {}
    for table in table_list:
        if table.upper() == "ADMISSIONS":
            chosen_activity_time[table] = ["concept:name", "time:timestamp"]
        elif table.upper() == "ICUSTAYS":
            chosen_activity_time[table] = ["concept:name", "time:timestamp"]
        elif tables_activities is None and tables_timestamps is None:
            detail_columns = []
            module = get_table_module(table)

            table_columns = extract_table_columns(db_cursor, module, table)

            try:
                detail_table = detail_tables[table]
            except KeyError:
                detail_table = None
            if detail_table is not None:
                detail_columns = extract_table_columns(
                    db_cursor, module, detail_table)

            table_columns = table_columns + detail_columns  # type: ignore

            time_columns = list(
                filter(lambda col: "time" in col or "date" in col, table_columns))
            activity_columns = list(filter(lambda col: "time" not in col and "date" not in col
                                           and "id" not in col, table_columns))

            logger.info(
                "The table %s includes the following activity columns: ", table)
            logger.info('[%s]' % ', '.join(map(str, activity_columns)))
            chosen_activity_column = input("Choose the activity column:")

            if len(time_columns) == 1:
                chosen_time_column = time_columns[0]
            else:
                logger.info(
                    "The table %s includes multiple timestamps: ", table)
                logger.info('[%s]' % ', '.join(map(str, time_columns)))
                chosen_time_column = input("Choose the timestamp column:")
            chosen_activity_time[table] = [
                chosen_activity_column, chosen_time_column]
        elif tables_activities is not None and tables_timestamps is not None:
            table_index = table_list.index(table)
            chosen_activity_time[table] = [tables_activities[table_index],
                                           tables_timestamps[table_index]]

    return chosen_activity_time


def extract_tables(db_cursor: cursor, table_list: List[str], hospital_admission_ids: List[float],
                   chosen_activity_time: Optional[dict], cohort: pd.DataFrame)\
                    -> pd.DataFrame:
    """
    Extracts given tables from the database and generates an event log
    """

    final_log = pd.DataFrame()

    for table in table_list:

        module = get_table_module(table)

        if module == "mimiciv_ed":
            ed_stays = extract_emergency_department_stays_for_admission_ids(
                db_cursor, hospital_admission_ids)
            ed_stays = ed_stays[["subject_id", "hadm_id", "stay_id"]]
            ed_stay_list = list(ed_stays["stay_id"])
            table_content = extract_ed_table_for_ed_stays(
                db_cursor, ed_stay_list, table)
            table_content = table_content.merge(
                ed_stays, on=["stay_id", "subject_id"], how="inner")
        elif table.upper() == "ADMISSIONS":
            table_content = extract_admission_events(db_cursor, cohort, False)
        elif table.upper() == "ICUSTAYS":
            table_content = extract_icustay_events(db_cursor, cohort)
        else:
            table_content = extract_table_for_admission_ids(
                db_cursor, hospital_admission_ids, module, table)

        try:
            detail_table = detail_tables[table]
        except KeyError:
            detail_table = None

        if detail_table is not None:
            if detail_table == "prescriptions":
                detail_content = extract_table_for_admission_ids(db_cursor, hospital_admission_ids,
                                                                 module, detail_table)
                detail_content = detail_content[['pharmacy_id', 'drug_type', 'drug', 'gsn',
                                                 'ndc', 'prod_strength', 'form_rx',
                                                 'dose_val_rx', 'dose_unit_rx',
                                                 'form_val_disp', 'form_unit_disp']]
            else:
                detail_content = extract_table(db_cursor, module, detail_table)
            detail_foreign_key = detail_foreign_keys[detail_table]
            if table.lower() == "hcpcsevents":
                table_content.rename(columns={"hcpcs_cd":"code"}, inplace=True)
            table_content = table_content.merge(detail_content,                    # type: ignore
                                                on=detail_foreign_key, how="left")  # type: ignore

        if chosen_activity_time is not None:
            table_content = table_content.rename(columns={
                chosen_activity_time[table][1]: "time:timestamp",
                chosen_activity_time[table][0]: "concept:name"})

        final_log = pd.concat([final_log, table_content])

    return final_log
