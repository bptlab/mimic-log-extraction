"""
Provides functionality for extracting a cohort defined by ICD and DRG codes, as well as patient ages
"""
import logging
from typing import List
import pandas as pd
from psycopg2.extensions import cursor

from extractor.constants import ADMISSION_CASE_NOTION, SUBJECT_CASE_NOTION
from .extraction_helper import (extract_admissions_for_admission_ids, extract_patients,
                                get_filename_string, extract_table_for_admission_ids)

logger = logging.getLogger('cli')


def extract_case_attributes(db_cursor: cursor, cohort: pd.DataFrame, case_notion: str,
                            case_attribute_list: List[str],
                            save_intermediate: bool) -> pd.DataFrame:
    """
    Extracts case attributes for a given cohort
    """

    logger.info("Begin extracting case attributes!")
    case_attributes = pd.DataFrame()
    if case_notion == SUBJECT_CASE_NOTION:
        subject_df = extract_patients(db_cursor)
        subject_ids = list(cohort["subject_id"].unique())
        subject_df = subject_df.loc[subject_df["subject_id"].isin(subject_ids)]
        case_attribute_list.append("subject_id")
        case_attributes = subject_df[case_attribute_list]
        case_attributes = case_attributes.set_index("subject_id")
    elif case_notion == ADMISSION_CASE_NOTION:
        hadm_ids = list(cohort["hadm_id"].unique())
        hadm_ids = [float(i) for i in hadm_ids]
        hadm_df = extract_admissions_for_admission_ids(db_cursor, hadm_ids)
        cohort_data = cohort[["hadm_id", "age", "gender"]]
        hadm_df = hadm_df.merge(cohort_data, on="hadm_id", how="inner")
        icd_codes = extract_table_for_admission_ids(db_cursor, hadm_ids,
                                                    "mimiciv_hosp", "diagnoses_icd")
        icd_codes = icd_codes.sort_values(["hadm_id", "seq_num"])
        icd_codes = icd_codes[["hadm_id", "icd_code"]]
        icd_codes = icd_codes.groupby(
            "hadm_id")["icd_code"].apply(list)  # type: ignore
        drg_codes = extract_table_for_admission_ids(
            db_cursor, hadm_ids, "mimiciv_hosp", "drgcodes")
        drg_codes = drg_codes[["hadm_id", "drg_code"]]
        drg_codes = drg_codes.groupby(
            "hadm_id")["drg_code"].apply(list)  # type: ignore
        hadm_df = hadm_df.merge(icd_codes, on="hadm_id", how="inner")
        hadm_df = hadm_df.merge(drg_codes, on="hadm_id", how="inner")
        case_attribute_list.append("hadm_id")

        case_attributes = hadm_df[case_attribute_list]
        case_attributes = case_attributes.set_index("hadm_id")

    if save_intermediate:
        filename = get_filename_string("case_attributes", ".csv")
        case_attributes.to_csv("output/" + filename)

    logger.info("Done extracting case attributes!")
    return case_attributes
