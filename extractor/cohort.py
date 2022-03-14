"""
Provides functionality for extracting a cohort defined by ICD and DRG codes, as well as patient ages
"""
import pandas as pd

from .helper import (extract_drgs, extract_icds, filter_icd_df, filter_drg_df, get_filename_string,
                     extract_icd_descriptions, default_icd_list, default_drg_list)


def extract_cohort(db_cursor, icd_codes, drg_codes, ages) -> pd.DataFrame:
    """
    Selects a cohort of patient filtered by age,
    as well as ICD and DRG codes.
    Todo: ignores age filter so far
    """

    print("Begin extracting cohort!")

    if icd_codes is not None:
        print("Using supplied ICD codes for cohort...")
        icd_filter_list = icd_codes
    else:
        print("Using default ICD codes for cohort...")
        icd_filter_list = default_icd_list

    if drg_codes is not None:
        print("Using supplied DRG codes for cohort...")
        drg_filter_list = drg_codes
    else:
        print("Using default DRG codes for cohort...")
        drg_filter_list = default_drg_list

    if ages is None or ages == ['']:
        # select all patients
        print("No age filter supplied.")
    else:
        # filter patients
        print("Age filter supplied.")

    drgs = extract_drgs(db_cursor)

    icds = extract_icds(db_cursor)

    desc_icd = extract_icd_descriptions(db_cursor)

    cohort = icds.merge(desc_icd, on="icd_code", how="inner")

    # Filter for relevant ICD codes
    cohort = filter_icd_df(icds=icds, icd_filter_list=icd_filter_list)

    cohort = cohort.loc[cohort["seq_num"] < 4]

    cohort = cohort.reset_index()
    cohort = cohort.drop("index", axis=1)

    drg_cohort = drgs.loc[drgs["hadm_id"].isin(list(cohort["hadm_id"]))]

    drg_cohort = drg_cohort.loc[drg_cohort["drg_type"] == "APR"]

    drg_filter_list_uppercase = [x.upper() for x in drg_filter_list]

    filtered_cohort = filter_drg_df(drg_cohort, drg_filter_list_uppercase)
    filtered_cohort = filtered_cohort.sort_values(["hadm_id", "drg_code"])
    filtered_cohort = filtered_cohort.reset_index()
    filtered_cohort.drop("index", axis=1, inplace=True)

    filename = get_filename_string("cohort_full", ".csv")
    filtered_cohort.to_csv("output/" + filename)

    print("Done extracting cohort!")

    return filtered_cohort
