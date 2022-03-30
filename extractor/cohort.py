"""
Provides functionality for extracting a cohort defined by ICD and DRG codes, as well as patient ages
"""
import logging
import pandas as pd
from .helper import (extract_drgs, extract_icds, filter_icd_df, filter_drg_df, get_filename_string,
                    extract_admissions, default_icd_list, default_drg_list)

logger = logging.getLogger('cli')


def extract_cohort(db_cursor, icd_codes, icd_version, drg_codes, drg_type, ages) -> pd.DataFrame:
    """
    Selects a cohort of patient filtered by age,
    as well as ICD and DRG codes.
    Todo: ignores age filter so far
    """

    logger.info("Begin extracting cohort!")

    if icd_codes is not None:
        if "IGNORE" in icd_codes:
            logger.info("Skipping ICD code filtering...")
            icd_filter_list = icd_codes
        else:
            logger.info("Using supplied ICD codes for cohort...")
            icd_filter_list = icd_codes
    else:
        logger.info("Using default ICD codes for cohort...")
        icd_filter_list = default_icd_list

    if drg_codes is not None:
        if "IGNORE" in drg_codes:
            logger.info("Skipping DRG code filtering...")
            drg_filter_list = drg_codes
        else:
            logger.info("Using supplied DRG codes for cohort...")
            drg_filter_list = drg_codes
    else:
        logger.info("Using default DRG codes for cohort...")
        drg_filter_list = default_drg_list

    if ages is None or ages == ['']:
        # select all patients
        logger.info("No age filter supplied.")
    else:
        # filter patients
        logger.info("Age filter supplied.")

    cohort = extract_admissions(db_cursor)
    cohort = cohort[["subject_id", "hadm_id"]]

    drgs = extract_drgs(db_cursor)

    icds = extract_icds(db_cursor)

    # Filter for relevant ICD codes
    if "IGNORE" not in icd_filter_list:
        icd_cohort = filter_icd_df(icds=icds, icd_filter_list=icd_filter_list,
                                    icd_version=icd_version)
        icd_cohort = icd_cohort.loc[icd_cohort["seq_num"] < 4]
        icd_cohort = icd_cohort.reset_index().drop("index", axis=1)
        icd_cohort["icd_code"] = icd_cohort["icd_code"].str.replace(" ", "")
        icd_cohort = icd_cohort[["hadm_id", "icd_code"]].groupby("hadm_id").agg(list).reset_index()
        cohort = cohort.loc[cohort["hadm_id"].isin(list(icd_cohort["hadm_id"]))]
        cohort = cohort.merge(icd_cohort, on="hadm_id", how="inner")

    # Filter for relevnt DRG codes
    if "IGNORE" not in drg_filter_list:
        drgs = drgs.loc[drgs["drg_type"] == drg_type]
        drg_cohort = filter_drg_df(drgs, drg_filter_list)
        cohort = cohort.loc[cohort["hadm_id"].isin(list(drg_cohort["hadm_id"]))]
        cohort = cohort.merge(drg_cohort, on=["subject_id", "hadm_id"], how="inner")


    cohort = cohort.reset_index().drop("index", axis=1)
    filename = get_filename_string("cohort_full", ".csv")
    cohort.to_csv("output/" + filename)

    logger.info("Done extracting cohort!")

    return cohort
