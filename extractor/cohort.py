
import numpy as np
from psycopg2 import connect
import pandas as pd
import numpy as np

from .helper import (extract_drgs, extract_icds, filter_icd_df, filter_drg_df, get_filename_string,
                     extract_icd_descriptions, default_icd_list, default_drg_list)


def extract_cohort(db_cursor, icd_codes, drg_codes, ages) -> pd.DataFrame:
    """
    Selects a cohort of patient filtered by age,
    as well as ICD and DRG codes.
    todo: ignores age filter so far
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

    hf = icds.merge(desc_icd, on="icd_code", how="inner")

    # Filter for relevant ICD codes
    hf = filter_icd_df(icds=icds, icd_filter_list=icd_filter_list)

    hf = hf.loc[hf["seq_num"] < 4]

    hf = hf.reset_index()
    hf = hf.drop("index", axis=1)

    hf_drg = drgs.loc[drgs["hadm_id"].isin(list(hf["hadm_id"]))]

    hf_drg = hf_drg.loc[hf_drg["drg_type"] == "APR"]

    drg_filter_list_uppercase = [x.upper() for x in drg_filter_list]

    hf_filter = filter_drg_df(hf_drg, drg_filter_list_uppercase)
    hf_filter = hf_filter.sort_values(["hadm_id", "drg_code"])
    hf_filter = hf_filter.reset_index()
    hf_filter.drop("index", axis=1, inplace=True)

    filename = get_filename_string("cohort_full", ".csv")

    hf_filter.to_csv("output/" + filename)

    print("Done extracting cohort!")

    return hf_filter
