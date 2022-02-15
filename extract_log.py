import string
from typing import List, Tuple
import numpy
from psycopg2 import connect
import pandas as pd
import pm4py
import numpy as np
import pandasql as ps
from pm4py.objects.conversion.log import converter as log_converter

import argparse

from extractor import extract_cohort

# todo: fixed requirements.txt
# todo: setup.py

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


# ask for database connection or read from environment
def ask_db_settings(args) -> Tuple(str, str, str, str):
    print("determining and establishing database connection...")
    db_name = args.db_name if args.db_name is not None else str(
        input("Enter Database Name"))
    db_host = args.db_host if args.db_host is not None else str(
        input("Enter Database Host"))
    db_user = args.db_user if args.db_user is not None else str(
        input("Enter Database User"))
    db_pw = args.db_pw if args.db_pw is not None else str(
        input("Enter Database Password"))
    return db_name, db_host, db_user, db_pw

# create db connection


def create_db_connection(db_name, db_host, db_user, db_pw):
    con = connect(db_name, db_host, db_user, db_pw)
    con.set_client_encoding('utf8')
    return con

# ask for patient cohorts


def ask_cohorts() -> Tuple(List[str], List[str], List[str]):
    print("determining patient cohort(s)...")
    icd_string = args.icd if args.icd is not None else str(
        input("Enter ICD Code(s) seperated by comma"))
    icd_codes = icd_string.split(',')

    drg_string = args.drg if args.drg is not None else str(
        input("Enter DRG Code(s) seperated by comma"))
    drg_codes = drg_string.split(',')

    age_string = args.age if args.age is not None else str(
        input("Enter Patient Age(s) seperated by comma"))
    ages = age_string.split(',')

    return icd_codes, drg_codes, ages

# ask for case notion


def ask_case_notion():
    return None

# ask for case attributes


def ask_case_attributes():
    return None

# ask for event types


def ask_event_types():
    return None

# ask for event attributes


def ask_event_attributes():
    return None


if __name__ == "__main__":
    # database conn
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
