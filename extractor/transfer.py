"""Provides functionality to generate transfer event logs for a given cohort"""
import logging
import pandas as pd
from psycopg2.extensions import cursor
from .extraction_helper import (
    get_filename_string, extract_transfers_for_admission_ids)


logger = logging.getLogger('cli')


def extract_transfer_events(db_cursor: cursor, cohort: pd.DataFrame,
                            save_intermediate: bool) -> pd.DataFrame:
    """
    Extracts transfer events for a given cohort
    """

    logger.info("Begin extracting transfer events!")

    hospital_admission_ids = list(cohort["hadm_id"].unique())
    hospital_admission_ids = [float(i) for i in hospital_admission_ids]
    admission_ids = hospital_admission_ids

    transfers = extract_transfers_for_admission_ids(db_cursor, admission_ids)

    transfers.loc[transfers["eventtype"] == "discharge",
                  "careunit"] = "Discharge"  # type: ignore

    transfers = transfers.sort_values(["hadm_id", "intime"])

    transfers = transfers.reset_index().drop("index", axis=1)

    transfers = transfers.rename(
        {"careunit": "concept:name", "intime": "time:timestamp"}, axis=1)
    if save_intermediate:
        filename = get_filename_string("transfer_log", ".csv")
        transfers.to_csv("output/" + filename)

    logger.info("Done extracting transfer events!")

    return transfers
