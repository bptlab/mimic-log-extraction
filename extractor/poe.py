"""Provides functionality to generate POE event logs for a given cohort"""
import logging
import pandas as pd
from psycopg2.extensions import cursor
from .extraction_helper import (extract_table_for_subject_ids, get_filename_string,
                                extract_poe_for_admission_ids,
                                extract_table_for_admission_ids)


logger = logging.getLogger('cli')


def extract_poe_events(db_cursor: cursor, cohort: pd.DataFrame, include_medications: bool,
                       save_intermediate: bool) -> pd.DataFrame:
    """
    Extracts poe events for a given cohort
    """

    logger.info("Begin extracting POE events!")

    hospital_admission_ids = list(cohort["hadm_id"].unique())
    hospital_admission_ids = [float(i) for i in hospital_admission_ids]
    poe = extract_poe_for_admission_ids(db_cursor, hospital_admission_ids)

    if include_medications is True:
        pharmacy = extract_table_for_admission_ids(db_cursor, hospital_admission_ids,
                                                   'mimiciv_hosp', 'pharmacy')
        prescriptions = extract_table_for_admission_ids(db_cursor, hospital_admission_ids,
                                                        'mimiciv_hosp', 'prescriptions')
        prescriptions = prescriptions[['pharmacy_id', 'drug_type', 'drug', 'gsn', 'ndc',
                                       'prod_strength', 'form_rx', 'dose_val_rx',
                                       'dose_unit_rx', 'form_val_disp', 'form_unit_disp']]
        emar = extract_table_for_admission_ids(db_cursor, hospital_admission_ids,
                                               'mimiciv_hosp', "emar")
        emar_subject_ids = list(emar["subject_id"].unique())
        emar_detail = extract_table_for_subject_ids(db_cursor, emar_subject_ids, 'mimiciv_hosp',
                                                    "emar_detail")
        emar = emar.merge(emar_detail, on=["emar_id", "subject_id", "emar_seq", "pharmacy_id"],
                          how="left")
        emar.rename(columns={"medication": "emar_medication",
                    "route": "emar_route"}, inplace=True)
        medications = pharmacy.merge(
            prescriptions, on=["pharmacy_id"], how="left")
        medications = medications.merge(
            emar, on=["poe_id", "hadm_id", "subject_id", "pharmacy_id"], how="left")
        medications.drop_duplicates("poe_id", inplace=True)  # type: ignore
        poe_with_medications = poe.merge(medications, on=["poe_id", "subject_id", "hadm_id"],
                                         how="left")
        poe_with_medications.loc[poe_with_medications["order_type"] == "Medications",
                                 "order_subtype"] = poe_with_medications["medication"]
        filename = get_filename_string("poe_with_medications_log", ".csv")
        poe_with_medications.to_csv("output/" + filename)
        logger.info("Done extracting POE events!")

        poe_with_medications.rename(columns={"order_subtype": "concept:name",
                                             "ordertime": "time:timestamp"}, inplace=True)

        return poe_with_medications

    poe.rename(columns={"order_subtype": "concept:name", "ordertime": "time:timestamp"},
               inplace=True)
    if save_intermediate:
        filename = get_filename_string("poe_log", ".csv")
        poe.to_csv("output/" + filename)

    logger.info("Done extracting POE events!")
    return poe
