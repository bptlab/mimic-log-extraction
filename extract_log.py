from typing import List, Tuple
from psycopg2 import connect

import argparse

from extractor import extract_cohort

parser = argparse.ArgumentParser(description='Optional app description')

# Database Parameters
parser.add_argument('--db_name', type=str, help='Database Name')
parser.add_argument('--db_host', type=str, help='Database Host')
parser.add_argument('--db_user', type=str, help='Database User')
parser.add_argument('--db_pw', type=str, help='Database Password')

# Patient Cohort Parameters
parser.add_argument('--icd', type=str, help='ICD code(s) of cohort')
parser.add_argument('--drg', type=str, help='DRG code(s) of cohort')
parser.add_argument('--age', type=str, help='Patient Age of cohort')


def ask_db_settings(args) -> Tuple[str, str, str, str]:
    """
    Ask for database connection or read from environment
    """
    print("determining and establishing database connection...")
    db_name = args.db_name if args.db_name is not None else str(
        input("Enter Database Name:\n"))
    db_host = args.db_host if args.db_host is not None else str(
        input("Enter Database Host:\n"))
    db_user = args.db_user if args.db_user is not None else str(
        input("Enter Database User:\n"))
    db_pw = args.db_pw if args.db_pw is not None else str(
        input("Enter Database Password:\n"))
    return db_name, db_host, db_user, db_pw


def create_db_connection(db_name, db_host, db_user, db_pw):
    """
    Create database connection with supplied parameters
    """
    con = connect(db_name, db_host, db_user, db_pw)
    con.set_client_encoding('utf8')
    return con


def ask_cohorts() -> Tuple[List[str], List[str], List[str]]:
    """
    Ask for patient cohort filters
    todo: age is always None, so far
    """
    print("determining patient cohort(s)...")
    icd_string = args.icd if args.icd is not None else str(
        input("Enter ICD Code(s) seperated by comma:\n"))
    icd_codes = icd_string.split(',')

    drg_string = args.drg if args.drg is not None else str(
        input("Enter DRG Code(s) seperated by comma:\n"))
    drg_codes = drg_string.split(',')

    # todo: age is None as of yet
    age_string = args.age if args.age is not None else str(
        input("Enter Patient Age(s) seperated by comma:\n"))
    ages = age_string.split(',')

    return icd_codes, drg_codes, None


def ask_case_notion():
    """
    Ask for case notion
    todo: None per default
    """
    return None


def ask_case_attributes():
    """
    Ask for case attributes
    todo: None per default
    """
    return None


def ask_event_types():
    """
    Ask for event types
    todo: None per default
    """
    return None


def ask_event_attributes():
    """
    Ask for event attributes
    todo: None per default
    """
    return None


if __name__ == "__main__":
    args = parser.parse_args()
    db_name, db_host, db_user, db_pw = ask_db_settings(args)
    db_connection = create_db_connection(db_name, db_host, db_user, db_pw)
    db_cursor = db_connection.cursor()

    cohort_icd_codes, cohort_drg_codes, cohort_age = ask_cohorts()

    case_notion = ask_case_notion()

    case_attributes = ask_case_attributes()

    event_types = ask_event_types()

    event_attributes = ask_event_attributes()

    # build cohort
    cohort = extract_cohort(db_cursor, cohort_icd_codes,
                            cohort_drg_codes, cohort_age)
