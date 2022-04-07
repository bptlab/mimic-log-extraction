"""
Provides functionality for extracting a cohort defined by ICD and DRG codes, as well as patient ages
"""
import logging
import pandas as pd
from .helper import (extract_drgs, extract_icds, filter_icd_df, filter_drg_df, get_filename_string,
                     extract_admissions, extract_patients, filter_age_ranges)


# todo: add type annotations in method signatures

logger = logging.getLogger('cli')


def extract_cohort_for_ids(db_cursor, subject_ids, hadm_ids):
    """Selects a cohort of patients filteres by provided hospital admission and/or subject ids"""

    logger.info("Begin extracting cohort!")
    cohort = extract_admissions(db_cursor)
    if hadm_ids is not None:
        hadm_ids = hadm_ids.split(',')
        hadm_ids = [int(hadm_id) for hadm_id in hadm_ids]
        cohort = cohort.loc[cohort["hadm_id"].isin(hadm_ids)]
    if subject_ids is not None:
        subject_ids = subject_ids.split(',')
        subject_ids = [int(subject_id) for subject_id in subject_ids]
        cohort = cohort.loc[cohort["subject_id"].isin(subject_ids)]

    cohort = cohort[["subject_id", "hadm_id", "admittime"]]
    cohort["admityear"] = cohort["admittime"].dt.year  # type: ignore
    patients = extract_patients(db_cursor)
    patients = patients[["subject_id", "anchor_age", "anchor_year", "gender"]]
    cohort = cohort.merge(patients, on="subject_id", how="inner")
    cohort["age"] = cohort["anchor_age"] + cohort["admityear"]  # type: ignore
    cohort["age"] = cohort["age"] - cohort["anchor_year"]  # type: ignore
    cohort.drop(["admittime", "admityear", "anchor_age",
                "anchor_year"], axis=1, inplace=True)
    cohort = cohort.reset_index().drop("index", axis=1)
    filename = get_filename_string("cohort_full", ".csv")
    cohort.to_csv("output/" + filename)

    logger.info("Done extracting cohort!")
    return cohort


def extract_cohort(db_cursor, icd_codes, icd_version, icd_seq_num,
                   drg_codes, drg_type, ages) -> pd.DataFrame:
    """
    Selects a cohort of patient filtered by age,
    as well as ICD and DRG codes.
    Todo: ignores age filter so far
    """

    logger.info("Begin extracting cohort!")

    if icd_codes is None:
        logger.info("Skipping ICD code filtering...")
    else:
        logger.info("Using supplied ICD codes for cohort...")
        icd_filter_list = icd_codes


    if drg_codes is None:
        logger.info("Skipping DRG code filtering...")
    else:
        logger.info("Using supplied DRG codes for cohort...")
        drg_filter_list = drg_codes

    cohort = extract_admissions(db_cursor)
    cohort = cohort[["subject_id", "hadm_id", "admittime"]]
    cohort["admityear"] = cohort["admittime"].dt.year  # type: ignore

    patients = extract_patients(db_cursor)
    patients = patients[["subject_id", "anchor_age", "anchor_year", "gender"]]
    cohort = cohort.merge(patients, on="subject_id", how="inner")
    cohort["age"] = cohort["anchor_age"] + cohort["admityear"]  # type: ignore
    cohort["age"] = cohort["age"] - cohort["anchor_year"]  # type: ignore
    cohort.drop(["admittime", "admityear", "anchor_age",
                "anchor_year"], axis=1, inplace=True)

    if ages is None or ages == ['']:
        # select all patients
        logger.info("No age filter supplied.")
    else:
        # filter patients
        cohort = filter_age_ranges(cohort, ages)
        logger.info("Age filter supplied.")

    drgs = extract_drgs(db_cursor)

    icds = extract_icds(db_cursor)

    # Filter for relevant ICD codes
    if icd_codes is not None:
        icd_cohort = filter_icd_df(icds=icds, icd_filter_list=icd_filter_list,
                                   icd_version=icd_version)
        icd_cohort = icd_cohort.loc[icd_cohort["seq_num"] <= icd_seq_num]
        icd_cohort = icd_cohort.reset_index().drop("index", axis=1)
        icd_cohort["icd_code"] = icd_cohort["icd_code"].str.replace(  # type: ignore
            " ", "")
        icd_cohort = icd_cohort[["hadm_id", "icd_code"]].groupby(
            "hadm_id").agg(list).reset_index()
        cohort = cohort.loc[cohort["hadm_id"].isin(
            list(icd_cohort["hadm_id"]))]
        cohort = cohort.merge(icd_cohort, on="hadm_id", how="inner")

    # Filter for relevnt DRG codes
    if drg_codes is not None:
        drgs = drgs.loc[drgs["drg_type"] == drg_type]
        drg_cohort = filter_drg_df(drgs, drg_filter_list)
        cohort = cohort.loc[cohort["hadm_id"].isin(
            list(drg_cohort["hadm_id"]))]
        cohort = cohort.merge(
            drg_cohort, on=["subject_id", "hadm_id"], how="inner")

    cohort = cohort.reset_index().drop("index", axis=1)
    filename = get_filename_string("cohort_full", ".csv")
    cohort.to_csv("output/" + filename)

    logger.info("Done extracting cohort!")

    return cohort
