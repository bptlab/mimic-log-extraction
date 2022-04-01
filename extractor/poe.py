"""Provides functionality to generate POE event logs for a given cohort"""
import logging
import pandas as pd
from .helper import (get_filename_string, extract_poe_for_admission_ids,
                    extract_table_for_admission_ids)


logger = logging.getLogger('cli')

# todo: add type annotations in method signatures

def extract_poe_events(db_cursor, cohort, include_medications) -> pd.DataFrame:
    """
    Extracts poe events for a given cohort
    """

    logger.info("Begin extracting POE events!")

    hospital_admission_ids = list(cohort["hadm_id"].unique())
    hospital_admission_ids = [float(i) for i in hospital_admission_ids]
    admission_ids = hospital_admission_ids

    poe = extract_poe_for_admission_ids(db_cursor, admission_ids)

    if include_medications is True:
        pharmacy = extract_table_for_admission_ids(db_cursor, hospital_admission_ids,
        'mimic_hosp', 'pharmacy')
        prescriptions = extract_table_for_admission_ids(db_cursor, hospital_admission_ids,
        'mimic_hosp', 'prescriptions')
        prescriptions = prescriptions[['pharmacy_id', 'drug_type', 'drug', 'gsn', 'ndc',
        'prod_strength','form_rx', 'dose_val_rx', 'dose_unit_rx', 'form_val_disp',
        'form_unit_disp']]
        medications = pharmacy.merge(prescriptions, on=["pharmacy_id"], how="left")
        medications.drop_duplicates("poe_id", inplace=True)
        poe_with_medications = poe.merge(medications, on=["poe_id", "subject_id", "hadm_id"],
                                        how="left")
        poe_with_medications.loc[poe_with_medications["order_type"] == "Medications",
                                "order_subtype"] = poe_with_medications["medication"]
        filename = get_filename_string("poe_with_medications_log", ".csv")
        poe_with_medications.to_csv("output/" + filename)
        logger.info("Done extracting POE events!")
        return poe_with_medications

    filename = get_filename_string("poe_log", ".csv")
    poe.to_csv("output/" + filename)
    logger.info("Done extracting POE events!")
    return poe
