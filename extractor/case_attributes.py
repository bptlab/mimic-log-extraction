"""
Provides functionality for extracting a cohort defined by ICD and DRG codes, as well as patient ages
"""
import logging
import pandas as pd
from .helper import (extract_admissions_for_admission_ids, extract_patients, get_filename_string)

logger = logging.getLogger('cli')


def extract_case_attributes(db_cursor, cohort, case_notion, case_attribute_list) -> pd.DataFrame:
    """
    Extracts case attributes for a given cohort
    """

    logger.info("Begin extracting case attributes!")
    case_attributes = pd.DataFrame()
    if case_notion == "SUBJECT":
        subject_df = extract_patients(db_cursor)
        subject_ids = list(cohort["subject_id"].unique())
        subject_df = subject_df.loc[subject_df["subject_id"].isin(subject_ids)]
        case_attribute_list.append("subject_id")
        case_attributes = subject_df[case_attribute_list]
        case_attributes = case_attributes.set_index("subject_id")
    elif case_notion == "HOSPITAL ADMISSION":
        hadm_ids = list(cohort["hadm_id"].unique())
        hadm_ids = [float(i) for i in hadm_ids]
        hadm_df = extract_admissions_for_admission_ids(db_cursor, hadm_ids)
        case_attribute_list.append("hadm_id")
        case_attributes = hadm_df[case_attribute_list]
        case_attributes = case_attributes.set_index("hadm_id")

    filename = get_filename_string("case_attributes", ".csv")
    case_attributes.to_csv("output/" + filename)
    logger.info("Done extracting case attributes!")
    return case_attributes
