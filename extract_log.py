#!/usr/bin/env python3

"""
Provides the main CLI functionality for extracting configurable event logs out of a Mimic Database
"""
import argparse
import logging
from typing import List, Optional
import yaml

from pm4py.objects.conversion.log import converter as log_converter  # type: ignore
from pm4py.objects.log.exporter.xes import exporter as xes_exporter  # type: ignore

from extractor.transfer import extract_transfer_events
from extractor.tables import extract_table_events
from extractor.poe import extract_poe_events
from extractor.extraction_helper import get_filename_string
from extractor.event_attributes import extract_event_attributes
from extractor.admission import extract_admission_events
from extractor.case_attributes import extract_case_attributes
from extractor.cli_helper import ask_event_attributes, create_db_connection,\
    parse_or_ask_case_attributes, parse_or_ask_case_notion, parse_or_ask_cohorts,\
    parse_or_ask_db_settings, parse_or_ask_event_type, parse_or_ask_low_level_tables
from extractor.cohort import extract_cohort, extract_cohort_for_ids
from extractor.constants import ADDITIONAL_ATTRIBUTES_QUESTION, ADMISSION_CASE_KEY,\
    ADMISSION_CASE_NOTION, ADMISSION_EVENT_TYPE, INCLUDE_MEDICATION_QUESTION, OTHER_EVENT_TYPE,\
    POE_EVENT_TYPE, SUBJECT_CASE_KEY, SUBJECT_CASE_NOTION, TRANSFER_EVENT_TYPE

formatter = logging.Formatter(
    fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger = logging.getLogger('cli')
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

parser = argparse.ArgumentParser(
    description='A CLI tool for extracting event logs out of MIMIC Databases.')

# TODO: sanity check argument inputs before using later on!

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
parser.add_argument('--tables_activities', type=str,
                    help='Activity Columns for Low level tables')
parser.add_argument('--tables_timestamps', type=str,
                    help='Timestamp Columns for Low level tables')

# Case Notion Parameter
parser.add_argument('--notion', type=str, help='Case Notion')

# Case Attribute Parameter
parser.add_argument('--case_attribute_list', type=str, help='Case Attributes')

# Config File Argument
parser.add_argument('--config', type=str,
                    help='Config file for providing all options via file')

# Argument to store intermediate dataframes to disk
parser.add_argument('--save_intermediate', action='store_true')
parser.add_argument('--ignore_intermediate',
                    dest='save_intermediate', action='store_false')
parser.set_defaults(save_intermediate=False)


if __name__ == "__main__":
    args = parser.parse_args()

    config: Optional[dict] = None
    if args.config is not None:
        with open(args.config, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)

    # Should intermediate dataframes be saved?
    if config is not None and config["save_intermediate"] is not None:
        SAVE_INTERMEDIATE: bool = config['save_intermediate']
    else:
        SAVE_INTERMEDIATE = args.save_intermediate

    # Create database connection
    db_name, db_host, db_user, db_pw = parse_or_ask_db_settings(args, config)
    db_connection = create_db_connection(db_name, db_host, db_user, db_pw)
    db_cursor = db_connection.cursor()

    # Determine Cohort
    if args.subject_ids is None and args.hadm_ids is None:
        cohort_icd_codes, cohort_icd_version, cohort_icd_seq_num, cohort_drg_codes, \
            cohort_drg_type, cohort_age = parse_or_ask_cohorts(args, config)
    # Determine case notion
    determined_case_notion = parse_or_ask_case_notion(args, config)

    # Determine case attributes
    case_attribute_list = parse_or_ask_case_attributes(args,
                                                       determined_case_notion, config)

    event_type = parse_or_ask_event_type(args, config)

    # build cohort
    if args.subject_ids is None and args.hadm_ids is None:
        cohort = extract_cohort(db_cursor, cohort_icd_codes, cohort_icd_version,
                                cohort_icd_seq_num, cohort_drg_codes, cohort_drg_type,
                                cohort_age, SAVE_INTERMEDIATE)
    else:
        cohort = extract_cohort_for_ids(
            db_cursor, args.subject_ids, args.hadm_ids, SAVE_INTERMEDIATE)

    # extract case attributes
    if case_attribute_list is not None:
        case_attributes = extract_case_attributes(
            db_cursor, cohort, determined_case_notion, case_attribute_list, SAVE_INTERMEDIATE)

    if event_type == ADMISSION_EVENT_TYPE:
        events = extract_admission_events(db_cursor, cohort, SAVE_INTERMEDIATE)
    elif event_type == TRANSFER_EVENT_TYPE:
        events = extract_transfer_events(db_cursor, cohort, SAVE_INTERMEDIATE)
    elif event_type == POE_EVENT_TYPE:
        if config is not None and config["include_medications"] is not None:
            SHOULD_INCLUDE_MEDICATIONS: bool = config['include_medications']
        else:
            include_medications = input(INCLUDE_MEDICATION_QUESTION).upper()
            SHOULD_INCLUDE_MEDICATIONS = include_medications == "Y"

        events = extract_poe_events(
            db_cursor, cohort, SHOULD_INCLUDE_MEDICATIONS, SAVE_INTERMEDIATE)
    elif event_type == OTHER_EVENT_TYPE:
        tables_to_extract = parse_or_ask_low_level_tables(args, config)
        if args.tables_activities is not None:
            TABLES_ACTIVITIES = args.tables_activities.split(',')
        else:
            TABLES_ACTIVITIES = None
        if args.tables_timestamps is not None:
            TABLES_TIMESTAMPS = args.tables_timestamps.split(',')
        else:
            TABLES_TIMESTAMPS = None
        events = extract_table_events(db_cursor, cohort, tables_to_extract,
                                      TABLES_ACTIVITIES, TABLES_TIMESTAMPS, SAVE_INTERMEDIATE)

    if config is not None and config["additional_event_attributes"] is not None:
        additional_attributes: List[dict] = config['additional_event_attributes']
        for attribute in additional_attributes:
            events = extract_event_attributes(db_cursor, events, attribute['start_column'],
                                              attribute['end_column'], attribute['time_column'],
                                              attribute['table_to_aggregate'],
                                              attribute['column_to_aggregate'],
                                              attribute['aggregation_method'],
                                              attribute.get('filter_column'),
                                              attribute.get('filter_values'))
    else:
        event_attribute_decision = input(ADDITIONAL_ATTRIBUTES_QUESTION)
        while event_attribute_decision.upper() == "Y":
            start_column, end_column, time_column, table_to_aggregate, column_to_aggregate,\
                aggregation_method, filter_column, filter_values = ask_event_attributes(db_cursor,
                                                                                        events)
            events = extract_event_attributes(db_cursor, events, start_column, end_column,
                                              time_column, table_to_aggregate, column_to_aggregate,
                                              aggregation_method, filter_column, filter_values)
            event_attribute_decision = input(ADDITIONAL_ATTRIBUTES_QUESTION)

    if SAVE_INTERMEDIATE:
        csv_filename = get_filename_string(
            "event_attribute_enhanced_log", ".csv")
        events.to_csv("output/" + csv_filename)

    # set case id key based on determined case notion
    if determined_case_notion == SUBJECT_CASE_NOTION:
        CASE_ID_KEY = SUBJECT_CASE_KEY
    elif determined_case_notion == ADMISSION_CASE_NOTION:
        CASE_ID_KEY = ADMISSION_CASE_KEY

    # rename every case attribute to have case prefix
    if case_attribute_list is not None and case_attributes is not None:
        # join case attr to events
        if determined_case_notion == SUBJECT_CASE_NOTION:
            events = events.merge(
                case_attributes, on=SUBJECT_CASE_KEY, how='left')
        elif determined_case_notion == ADMISSION_CASE_NOTION:
            events = events.merge(
                case_attributes, on=ADMISSION_CASE_KEY, how='left')

        # rename case id key, as this will be affected too
        CASE_ID_KEY = 'case:' + CASE_ID_KEY
        for case_attr in case_attribute_list:
            events.rename(
                columns={case_attr: "case:" + case_attr}, inplace=True)

    parameters = {log_converter.Variants.TO_EVENT_LOG.value
                  .Parameters.CASE_ID_KEY: CASE_ID_KEY,
                  log_converter.Variants.TO_EVENT_LOG.value
                  .Parameters.CASE_ATTRIBUTE_PREFIX: 'case:'}
    event_log_object = log_converter.apply(
        events, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG)
    filename = get_filename_string("event_log", ".xes")
    xes_exporter.apply(event_log_object, "output/" + filename)
