"""
Provides functionality for extracting a cohort defined by ICD and DRG codes, as well as patient ages
"""
import logging
import pandas as pd
from .helper import (extract_drgs, extract_icds, filter_icd_df, filter_drg_df, get_filename_string,
                    extract_admissions, extract_patients, filter_age_ranges,
                    default_icd_list, default_drg_list)

logger = logging.getLogger('cli')


def extract_cohort(db_cursor, icd_codes, icd_version, icd_seq_num,
                    drg_codes, drg_type, ages) -> pd.DataFrame:
    """
    Selects a cohort of patient filtered by age,
    as well as ICD and DRG codes.
    Todo: ignores age filter so far
    """

    logger.info("Begin extracting cohort!")

    if icd_codes is not None:
        if "ALL" in icd_codes:
            logger.info("Skipping ICD code filtering...")
            icd_filter_list = icd_codes
        else:
            logger.info("Using supplied ICD codes for cohort...")
            icd_filter_list = icd_codes
    else:
        logger.info("Using default ICD codes for cohort...")
        icd_filter_list = default_icd_list

    if drg_codes is not None:
        if "ALL" in drg_codes:
            logger.info("Skipping DRG code filtering...")
            drg_filter_list = drg_codes
        else:
            logger.info("Using supplied DRG codes for cohort...")
            drg_filter_list = drg_codes
    else:
        logger.info("Using default DRG codes for cohort...")
        drg_filter_list = default_drg_list


    cohort = extract_admissions(db_cursor)
    cohort = cohort[["subject_id", "hadm_id", "admittime"]]
    cohort["admityear"] = cohort["admittime"].dt.year

    patients = extract_patients(db_cursor)
    patients = patients[["subject_id", "anchor_age", "anchor_year"]]
    cohort = cohort.merge(patients, on="subject_id", how="inner")
    cohort["age"] = cohort["anchor_age"] + cohort["admityear"] - cohort["anchor_year"]
    cohort.drop(["admittime", "admityear", "anchor_age", "anchor_year"], axis=1, inplace=True)

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
    if "ALL" not in icd_filter_list:
        icd_cohort = filter_icd_df(icds=icds, icd_filter_list=icd_filter_list,
                                    icd_version=icd_version)
        icd_cohort = icd_cohort.loc[icd_cohort["seq_num"] <= icd_seq_num]
        icd_cohort = icd_cohort.reset_index().drop("index", axis=1)
        icd_cohort["icd_code"] = icd_cohort["icd_code"].str.replace(" ", "") # type: ignore
        icd_cohort = icd_cohort[["hadm_id", "icd_code"]].groupby("hadm_id").agg(list).reset_index()
        cohort = cohort.loc[cohort["hadm_id"].isin(list(icd_cohort["hadm_id"]))]
        cohort = cohort.merge(icd_cohort, on="hadm_id", how="inner")

    # Filter for relevnt DRG codes
    if "ALL" not in drg_filter_list:
        drgs = drgs.loc[drgs["drg_type"] == drg_type]
        drg_cohort = filter_drg_df(drgs, drg_filter_list)
        cohort = cohort.loc[cohort["hadm_id"].isin(list(drg_cohort["hadm_id"]))]
        cohort = cohort.merge(drg_cohort, on=["subject_id", "hadm_id"], how="inner")


    cohort = cohort.reset_index().drop("index", axis=1)
    filename = get_filename_string("cohort_full", ".csv")
    cohort.to_csv("output/" + filename)

    logger.info("Done extracting cohort!")

    return cohort
