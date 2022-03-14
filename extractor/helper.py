"""
Provides helper methods for extraction of data frames from Mimic
"""
import logging
from typing import List
from datetime import datetime
import pandas as pd

logger = logging.getLogger('cli')


def extract_icd_descriptions(cursor) -> pd.DataFrame:
    cursor.execute("SELECT * FROM mimic_hosp.d_icd_diagnoses")
    desc_icd = cursor.fetchall()
    cols = list(map(lambda x: x[0], cursor.description))
    desc_icd = pd.DataFrame(desc_icd, columns=cols)
    desc_icd = desc_icd[["icd_code", "long_title"]]
    return desc_icd


def extract_icds(cursor) -> pd.DataFrame:
    cursor.execute('SELECT * FROM mimic_hosp.diagnoses_icd')
    icds = cursor.fetchall()
    cols = list(map(lambda x: x[0], cursor.description))
    icds = pd.DataFrame(icds, columns=cols)
    return icds


def extract_drgs(cursor) -> pd.DataFrame:
    cursor.execute("SELECT * from mimic_hosp.drgcodes")
    drgs = cursor.fetchall()
    cols = list(map(lambda x: x[0], cursor.description))
    drgs = pd.DataFrame(drgs, columns=cols)
    return drgs


def extract_admissions(cursor) -> pd.DataFrame:
    cursor.execute('SELECT * FROM mimic_core.admissions')
    adm = cursor.fetchall()
    cols = list(map(lambda x: x[0], cursor.description))
    adm = pd.DataFrame(adm, columns=cols)
    return adm


def extract_patients(cursor) -> pd.DataFrame:
    cursor.execute("SELECT * from mimic_core.patients")
    patients = cursor.fetchall()
    cols = list(map(lambda x: x[0], cursor.description))
    patients = pd.DataFrame(patients, columns=cols)
    return patients


def filter_icd_df(icds: pd.DataFrame, icd_filter_list: List[str]) -> pd.DataFrame:
    icd_filter = icds.loc[icds["icd_code"].str.contains(
        '|'.join(icd_filter_list))]
    return icd_filter


def filter_drg_df(hf_drg: pd.DataFrame, drg_filter_list: List[str]) -> pd.DataFrame:
    hf_filter = hf_drg.loc[hf_drg["description"].isin(
        drg_filter_list)]
    return hf_filter


def extract_triage_stays_for_ed_stays(db_cursor, ed_stays):
    db_cursor.execute(
        'SELECT * FROM mimic_ed.triage where stay_id = any(%s)', [ed_stays])
    triage = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    triage = pd.DataFrame(triage, columns=cols)
    return triage


def extract_emergency_department_stays_for_admission_ids(db_cursor, hospital_admission_ids):
    db_cursor.execute(
        'SELECT * FROM mimic_ed.edstays where hadm_id = any(%s)', [hospital_admission_ids])
    ed_stays = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    ed_stays = pd.DataFrame(ed_stays, columns=cols)
    return ed_stays


def extract_admissions_for_admission_ids(db_cursor, hospital_admission_ids):
    db_cursor.execute(
        'SELECT * FROM mimic_core.admissions where hadm_id = any(%s)', [hospital_admission_ids])
    adm = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    adm = pd.DataFrame(adm, columns=cols)
    return adm


def get_filename_string(file_name: str, file_ending: str) -> str:
    date = datetime.now().strftime("%d-%m-%Y-%H_%M_%S")
    return file_name + "_" + date + file_ending


default_icd_list = ["42821", "42823", "42831",
                    "42833", "42841", "42843",
                    "I5021", "I5023", "I5031",
                    "I5033", "I5041", "I5042",
                    "I5043", "I5811", "I5813"]

default_drg_list = ["Heart Failure",
                    "Cardiac Catheterization w/ Circ Disord Exc Ischemic Heart Disease",
                    "Percutaneous Cardiovascular Procedures w/o AMI",
                    "Cardiac Arrhythmia & Conduction Disorders",
                    "Acute Myocardial Infarction",
                    "Percutaneous Cardiovascular Procedures w/ AMI",
                    "Cardiac Catheterization for Ischemic Heart Disease",
                    "Cardiac Defibrillator & Heart Assist Anomaly",
                    "Cardiac Valve Procedures w/ Cardiac Catheterization",
                    "Coronary Bypass w/ Cardiac Cath Or Percutaneous Cardiac Procedure",
                    "Other Circulatory System Diagnoses"
                    ]
