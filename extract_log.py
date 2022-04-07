#!/usr/bin/env python3

"""
Provides the main CLI functionality for extracting configurable event logs out of a Mimic Database
"""
import argparse
import logging
import sys
from typing import List, Optional, Tuple

import yaml
from psycopg2 import connect


from extractor import (extract_cohort, extract_cohort_for_ids, extract_admission_events,
                       extract_transfer_events, extract_case_attributes,
                       subject_case_attributes, hadm_case_attributes, extract_poe_events,
                       extract_table_events, illicit_tables)

formatter = logging.Formatter(
    fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger = logging.getLogger('cli')
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

parser = argparse.ArgumentParser(
    description='A CLI tool for extracting event logs out of MIMIC Databases.')

# Todo: sanity check argument inputs before using later on!

# Database Parameters
parser.add_argument('--db_name', type=str, help='Database Name')
parser.add_argument('--db_host', type=str, help='Database Host')
parser.add_argument('--db_user', type=str, help='Database User')
parser.add_argument('--db_pw', type=str, help='Database Password')

# Patient Cohort Parameters
parser.add_argument('--subject_ids', type=str, help='Subject IDs of cohort')
parser.add_argument('--hadm_ids', type=str,
                    help='Hospital Admission IDs of cohort')
parser.add_argument('--icd', type=str, help='ICD code(s) of cohort')
parser.add_argument('--icd_version', type=int, help='ICD version')
parser.add_argument('--icd_sequence_number', type=int,
                    help='Ranking threshold of diagnosis')
parser.add_argument('--drg', type=str, help='DRG code(s) of cohort')
parser.add_argument('--drg_type', type=str, help='DRG type (HCFA, APR)')
parser.add_argument('--age', type=str, help='Patient Age of cohort')

# Event Type Parameter
parser.add_argument('--type', type=str, help='Event Type')
parser.add_argument('--tables', type=str, help='Low level tables')

# Case Notion Parameter
parser.add_argument('--notion', type=str, help='Case Notion')

# Case Attribute Parameter
parser.add_argument('--case_attribute_list', type=str, help='Case Attributes')

# Config File Argument
parser.add_argument('--config', type=str,
                    help='Config file for providing all options via file')


def parse_or_ask_db_settings(input_arguments,
                             config_object: Optional[dict]) -> Tuple[str, str, str, str]:
    """Parse database config or use flags/ask for input"""
    logger.info("Determining and establishing database connection...")
    if config_object is not None and config_object["db"] is not None:
        db_config = config_object["db"]
        # todo: check for missing config keys
        input_db_name = db_config["name"]
        input_db_host = db_config["host"]
        input_db_user = db_config["user"]
        input_db_password = db_config["pw"]
    else:
        input_db_name = input_arguments.db_name if input_arguments.db_name is not None else str(
            input("Enter Database Name:\n"))
        input_db_host = input_arguments.db_host if input_arguments.db_host is not None else str(
            input("Enter Database Host:\n"))
        input_db_user = input_arguments.db_user if input_arguments.db_user is not None else str(
            input("Enter Database User:\n"))
        input_db_password = input_arguments.db_pw if input_arguments.db_pw is not None else str(
            input("Enter Database Password:\n"))
    return input_db_name, input_db_host, input_db_user, input_db_password


def create_db_connection(name, host, user, password):
    """Create database connection with supplied parameters"""
    con = connect(dbname=name, host=host, user=user, password=password)
    con.set_client_encoding('utf8')
    return con


def ask_cohorts() -> Tuple[Optional[List[str]], Optional[int], Optional[int],
                           Optional[List[str]], Optional[str], Optional[List[str]]]:
    """Ask for patient cohort filters"""
    logger.info("Determining patient cohort...")

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
            input("Enter considered ranking threshold of diagnosis (1 is the highest priority):\n"))

    drg_string = args.drg if args.drg is not None else str(
        input("Enter DRG code(s) seperated by comma (Press enter to choose all):\n"))
    drg_codes = drg_string.split(',')
    drg_codes = None if drg_codes == [''] else drg_codes

    if drg_codes is None:
        drg_type = None
    else:
        drg_type = args.drg_type if args.drg_type is not None else str(
            input("Enter DRG ontology (HCFA, APR):\n"))

    age_string = args.age if args.age is not None else str(
        input("Enter Patient Age ranges seperated by comma, e.g. 0:20,50:90:\n"))
    ages = age_string.split(',')
    ages = None if ages == [''] else ages

    return icd_codes, icd_version, icd_seq_num, drg_codes, drg_type, ages


def ask_case_notion() -> str:
    """Ask for case notion: Subject_Id or Hospital_Admission_Id"""
    # todo: use ADT to encode case notion
    implemented_case_notions = ['SUBJECT', 'HOSPITAL ADMISSION']

    type_string = args.notion if args.notion is not None else str(
        input("Choose Case Notion: Subject, Hospital Admission ?\n"))

    if type_string.upper() not in implemented_case_notions:
        logger.error("The input provided was not in %s",
                     implemented_case_notions)
        sys.exit("No valid case notion provided.")
    return type_string.upper()


def ask_case_attributes(case_notion) -> List[str]:
    """Ask for case attributes"""
    logger.info("Determining case attributes...")
    logger.info("The following case notion was selected: %s", case_notion)
    logger.info("Available case attributes:")
    if case_notion == "SUBJECT":
        logger.info('[%s]' % ', '.join(map(str, subject_case_attributes)))
    elif case_notion == "HOSPITAL ADMISSION":
        logger.info('[%s]' % ', '.join(map(str, hadm_case_attributes)))
    attribute_string = args.case_attribute_list if args.case_attribute_list is not None else str(
        input("Enter case attributes seperated by comma (Press enter to choose all):\n"))
    attribute_list = attribute_string.split(',')
    if attribute_list == ['']:
        if case_notion == "SUBJECT":
            attribute_list = subject_case_attributes
        elif case_notion == "HOSPITAL ADMISSION":
            attribute_list = hadm_case_attributes

    return attribute_list


def ask_event_type():
    """Ask for event types: Admission, Transfer, ...?"""
    implemented_event_types = ['ADMISSION', 'TRANSFER', 'POE', 'OTHER']

    type_string = args.type if args.type is not None else str(
        input("Choose Event Type: Admission, Transfer, POE, ?\n"))

    if type_string.upper() not in implemented_event_types:
        logger.error("The input provided was not in %s",
                     implemented_event_types)
        sys.exit("No valid event type provided.")
    return type_string.upper()

def ask_tables():
    """Ask for low level tables: Chartevents, Procedureevents, Labevents, ...?"""
    table_string = args.tables if args.tables is not None else str(
    input("Enter low level tables: Chartevents, Procedureevents, Labevents, ... ?\n"))
    table_string = table_string.lower()
    table_list = table_string.split(",")
    if any(x in illicit_tables for x in table_list):
        sys.exit("Illicit tables provided.")
    table_list = list(map(lambda table: str.replace(table, " ", ""), table_list))
    return table_list

def ask_event_attributes():
    """Ask for event attributes"""
    # Todo: None per default
    return None


if __name__ == "__main__":
    args = parser.parse_args()

    config: Optional[dict] = None
    if args.config is not None:
        with open(args.config, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)

    db_name, db_host, db_user, db_pw = parse_or_ask_db_settings(
        args, config_object=config)
    db_connection = create_db_connection(db_name, db_host, db_user, db_pw)
    db_cursor = db_connection.cursor()

    if args.subject_ids is None and args.hadm_ids is None:
        cohort_icd_codes, cohort_icd_version, cohort_icd_seq_num, cohort_drg_codes, \
            cohort_drg_type, cohort_age = ask_cohorts()

    determined_case_notion = ask_case_notion()

    case_attribute_list = ask_case_attributes(determined_case_notion)

    event_type = ask_event_type()

    # event_attributes = ask_event_attributes()

    # build cohort
    if args.subject_ids is None and args.hadm_ids is None:
        cohort = extract_cohort(db_cursor, cohort_icd_codes, cohort_icd_version,
        cohort_icd_seq_num, cohort_drg_codes, cohort_drg_type, cohort_age)
    else:
        cohort = extract_cohort_for_ids(
            db_cursor, args.subject_ids, args.hadm_ids)
    # FOR TESTING PURPOSE, SHRINKS THE COHORT TO 50 CASES
    cohort = cohort[:50]  # type: ignore

    case_attributes = extract_case_attributes(
        db_cursor, cohort, determined_case_notion, case_attribute_list)

    if event_type == "ADMISSION":
        events = extract_admission_events(db_cursor, cohort)
    elif event_type == "TRANSFER":
        events = extract_transfer_events(db_cursor, cohort)
    elif event_type == "POE":
        include_medications = input("""POE links to medication tables \
(pharmacy, emar, prescriptions).\nShall the medication events be enhanced by the \
concrete medications prescribed? (Y/N):""")
        if include_medications == "Y":
            events = extract_poe_events(db_cursor, cohort, True)
        else:
            events = extract_poe_events(db_cursor, cohort, False)
    elif event_type == "OTHER":
        tables_to_extract = ask_tables()
        events = extract_table_events(db_cursor, cohort, tables_to_extract)
