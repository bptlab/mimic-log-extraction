"""
Provides methods for handling user input and parameter parsing
"""


import logging
import sys
from typing import List, Optional, Tuple

from psycopg2 import connect

from extractor.constants import ADMISSION_CASE_NOTION, ADMISSION_EVENT_TYPE,\
    OTHER_EVENT_TYPE, POE_EVENT_TYPE, SUBJECT_CASE_NOTION, TRANSFER_EVENT_TYPE
from extractor.extraction_helper import (subject_case_attributes, hadm_case_attributes,
                                         extract_table_columns, illicit_tables,
                                         get_table_module)

logger = logging.getLogger('cli')


def parse_or_ask_db_settings(args,
                             config_object: Optional[dict]) -> Tuple[str, str, str, str]:
    """Parse database config or use flags/ask for input"""
    logger.info("Determining and establishing database connection...")
    if config_object is not None and config_object["db"] is not None:
        db_config = config_object["db"]
        # TODO: check for missing config keys
        input_db_name = db_config["name"]
        input_db_host = db_config["host"]
        input_db_user = db_config["user"]
        input_db_password = db_config["pw"]
    else:
        input_db_name = args.db_name if args.db_name is not None else str(
            input("Enter Database Name:\n"))
        input_db_host = args.db_host if args.db_host is not None else str(
            input("Enter Database Host:\n"))
        input_db_user = args.db_user if args.db_user is not None else str(
            input("Enter Database User:\n"))
        input_db_password = args.db_pw if args.db_pw is not None else str(
            input("Enter Database Password:\n"))
    return input_db_name, input_db_host, input_db_user, input_db_password


def create_db_connection(name, host, user, password):
    """Create database connection with supplied parameters"""
    con = connect(dbname=name, host=host, user=user, password=password)
    con.set_client_encoding('utf8')
    return con


def parse_or_ask_cohorts(args, config_object: Optional[dict]) -> Tuple[
        Optional[List[str]], Optional[int],
        Optional[int], Optional[List[str]],
        Optional[str],
        Optional[List[str]]]:
    """Ask for patient cohort filters"""
    logger.info("Determining patient cohort...")

    if config_object is not None and config_object["icd_codes"] is not None:
        icd_codes = config_object['icd_codes']
        icd_codes = None if icd_codes == [''] else icd_codes
        icd_version = config_object["icd_version"]
        icd_seq_num = config_object["icd_seq_num"]
    else:
        icd_string = args.icd if args.icd is not None else str(
            input("Enter ICD code(s) seperated by comma (Press enter to choose all):\n"))
        icd_codes = icd_string.split(',')
        icd_codes = None if icd_codes == [''] else icd_codes

        if icd_codes is None:
            icd_version = None
            icd_seq_num = None
        else:
            icd_version = args.icd_version if args.icd_version is not None else int(
                input("Enter ICD version (9, 10, 0 for both):\n"))
            icd_seq_num = args.icd_sequence_number if args.icd_sequence_number is not None else int(
                input("Enter considered ranking threshold of \
                        diagnosis (1 is the highest priority):\n"))

    if config_object is not None and config_object["drg_codes"] is not None:
        drg_codes = config_object['drg_codes']
        drg_codes = None if drg_codes == [''] else drg_codes
        drg_type = config_object["drg_ontology"]
    else:
        drg_string = args.drg if args.drg is not None else str(
            input("Enter DRG code(s) seperated by comma (Press enter to choose all):\n"))
        drg_codes = drg_string.split(',')
        drg_codes = None if drg_codes == [''] else drg_codes

        if drg_codes is None:
            drg_type = None
        else:
            drg_type = args.drg_type if args.drg_type is not None else str(
                input("Enter DRG ontology (HCFA, APR):\n"))

    if config_object is not None and config_object["age"] is not None:
        ages = config_object['age']
    else:
        age_string = args.age if args.age is not None else str(
            input("Enter Patient Age ranges seperated by comma, e.g. 0:20,50:90:\n"))
        ages = age_string.split(',')
        ages = None if ages == [''] else ages

    return icd_codes, icd_version, icd_seq_num, drg_codes, drg_type, ages


def parse_or_ask_case_notion(args, config_object: Optional[dict]) -> str:
    """Ask for case notion: Subject_Id or Hospital_Admission_Id"""
    # TODO: use ADT to encode case notion
    logger.info("Determining case notion...")
    implemented_case_notions = [SUBJECT_CASE_NOTION, ADMISSION_CASE_NOTION]

    if config_object is not None and config_object["case_notion"] is not None:
        case_string = config_object['case_notion']
    else:
        case_string = args.notion if args.notion is not None else str(
            input("Choose Case Notion: Subject, Hospital Admission ?\n"))

    if case_string.upper() not in implemented_case_notions:
        logger.error("The input provided was not in %s",
                     implemented_case_notions)
        sys.exit("No valid case notion provided.")
    return case_string.upper()


def parse_or_ask_case_attributes(args, case_notion, config_object) -> Optional[List[str]]:
    """Ask for case attributes"""
    logger.info("Determining case attributes...")
    logger.info("The following case notion was selected: %s", case_notion)

    if config_object is not None and config_object["case_attributes"] is not None:
        attribute_list = config_object['case_attributes']
        if attribute_list == ['']:
            if case_notion == SUBJECT_CASE_NOTION:
                attribute_list = subject_case_attributes
            elif case_notion == ADMISSION_CASE_NOTION:
                attribute_list = hadm_case_attributes
    elif config_object is not None and config_object['case_attributes'] is None:
        attribute_list = None
    else:
        case_attribute_decision = input(
            "Do you want to skip case attribute extraction? (Y/N):").upper()
        if case_attribute_decision == "N":
            logger.info("Available case attributes:")
            if case_notion == SUBJECT_CASE_NOTION:
                logger.info('[%s]' % ', '.join(
                    map(str, subject_case_attributes)))
            elif case_notion == ADMISSION_CASE_NOTION:
                logger.info('[%s]' % ', '.join(map(str, hadm_case_attributes)))
            attribute_string = args.case_attribute_list if args.case_attribute_list is not None \
                else str(
                    input("Enter case attributes seperated by \
                            comma (Press enter to choose all):\n"))
            attribute_list = attribute_string.split(',')
            if attribute_list == ['']:
                if case_notion == SUBJECT_CASE_NOTION:
                    attribute_list = subject_case_attributes
                elif case_notion == ADMISSION_CASE_NOTION:
                    attribute_list = hadm_case_attributes
        else:
            attribute_list = None

    return attribute_list


def parse_or_ask_event_type(args, config_object: Optional[dict]):
    """Ask for event types: Admission, Transfer, ...?"""
    logger.info("Determining event type...")
    implemented_event_types = [ADMISSION_EVENT_TYPE, TRANSFER_EVENT_TYPE,
                               POE_EVENT_TYPE, OTHER_EVENT_TYPE]

    if config_object is not None and config_object["event_type"] is not None:
        type_string = config_object['event_type']
    else:
        type_string = args.type if args.type is not None else str(
            input("Choose Event Type: Admission, Transfer, POE, Other ?\n"))

    if type_string.upper() not in implemented_event_types:
        logger.error("The input provided was not in %s",
                     implemented_event_types)
        sys.exit("No valid event type provided.")
    return type_string.upper()


def parse_or_ask_low_level_tables(args, config_object: Optional[dict]):
    """Ask for low level tables: Chartevents, Procedureevents, Labevents, ...?"""
    logger.info("Determining low level tables...")
    if config_object is not None and config_object["low_level_tables"] is not None:
        table_list = config_object['low_level_tables']
    else:
        table_string = args.tables if args.tables is not None else str(
            input("Enter low level tables: Chartevents, Procedureevents, Labevents, ... ?\n"))
        table_string = table_string.lower()
        table_list = table_string.split(",")

    if any(x in illicit_tables for x in table_list):
        sys.exit("Illicit tables provided.")
    table_list = list(
        map(lambda table: str.replace(table, " ", ""), table_list))
    return table_list


def ask_event_attributes(db_cursor, event_log) -> Tuple[str, str, str, str, List[str],
                                                        str, Optional[str], Optional[List[str]]]:
    """Ask for event attributes"""
    logger.info("Determining event attributes...")
    table_columns = list(event_log.columns)
    time_columns = list(
        filter(lambda col: "time" in col or "date" in col, table_columns))

    logger.info("The following time columns are available:")
    logger.info(time_columns)

    start_col = input("""Enter the column name of the timestamp indicating the
start of the events: \n""")
    end_col = input("""Enter the column name of the timestamp indicating the
end of the events: \n""")
    table = input(
        """Enter the table name including the event attributes: \n""")
    module = get_table_module(table)
    table_columns = extract_table_columns(db_cursor, module, table)
    time_columns = list(
        filter(lambda col: "time" in col or "date" in col, table_columns))

    logger.info("The following time columns are available:")
    logger.info(time_columns)

    time_col = input("""Enter the column name of the timestamp in the table
which should be aggregated: \n""")
    column_to_agg = input(
        """Enter the column names which should be aggregated: \n""")
    column_to_agg_list = column_to_agg.split(",")
    agg_method = input("""Enter the aggregation method (Mean, Median,
Sum, Count, First): \n""")
    filter_col_string = input("""If only a part of the table should be aggregated,
you can provide a column to filter on (e.g. label in labevents for filtering specific
laboratory values): \n""")
    if filter_col_string == "":
        filter_col = None
    else:
        filter_col = filter_col_string
    filter_val_string = input("""Enter the values which should be used for filtering the
provided column (e.g. laboratory values, medications,
procedures, ...): \n""")
    if filter_val_string == "":
        filter_val = None
    else:
        filter_val = filter_val_string.split(",")

    return start_col, end_col, time_col, table, column_to_agg_list, \
        agg_method, filter_col, filter_val
