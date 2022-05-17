"""
Provides helper methods for extraction of data frames from Mimic
"""
import logging
from typing import List
from datetime import datetime
import pandas as pd
import pandasql as ps

logger = logging.getLogger('cli')

# TODO: add type annotations in method signatures


def extract_icd_descriptions(cursor) -> pd.DataFrame:
    """Extract ICD Codes and descriptions"""
    cursor.execute("SELECT * FROM mimic_hosp.d_icd_diagnoses")
    desc_icd = cursor.fetchall()
    cols = list(map(lambda x: x[0], cursor.description))
    desc_icd = pd.DataFrame(desc_icd, columns=cols)
    desc_icd = desc_icd[["icd_code", "long_title"]]
    return desc_icd


def extract_icds(cursor) -> pd.DataFrame:
    """Extract ICD Codes"""
    cursor.execute('SELECT * FROM mimic_hosp.diagnoses_icd')
    icds = cursor.fetchall()
    cols = list(map(lambda x: x[0], cursor.description))
    icds = pd.DataFrame(icds, columns=cols)
    return icds


def extract_drgs(cursor) -> pd.DataFrame:
    """Extract DRG Codes"""
    cursor.execute("SELECT * from mimic_hosp.drgcodes")
    drgs = cursor.fetchall()
    cols = list(map(lambda x: x[0], cursor.description))
    drgs = pd.DataFrame(drgs, columns=cols)
    return drgs


def extract_admissions(cursor) -> pd.DataFrame:
    """Extract admissions"""
    cursor.execute('SELECT * FROM mimic_core.admissions')
    adm = cursor.fetchall()
    cols = list(map(lambda x: x[0], cursor.description))
    adm = pd.DataFrame(adm, columns=cols)
    return adm


def extract_patients(cursor) -> pd.DataFrame:
    """Extract patients"""
    cursor.execute("SELECT * from mimic_core.patients")
    patients = cursor.fetchall()
    cols = list(map(lambda x: x[0], cursor.description))
    patients = pd.DataFrame(patients, columns=cols)
    return patients


def filter_age_ranges(cohort: pd.DataFrame, ages: List[str]) -> pd.DataFrame:
    """Filter a dataframe for a list of age ranges"""
    condition_list = []
    for age_interval in ages:
        age_min = age_interval.split(":", 1)[0]
        age_max = age_interval.split(":", 1)[1]
        condition_list.append(
            '(age >= ' + age_min + ' and age <= ' + age_max + ')')
    condition = 'or'.join(condition_list)
    cohort = cohort.query(condition)
    return cohort


def filter_icd_df(icds: pd.DataFrame, icd_filter_list: List[str], icd_version: int) -> pd.DataFrame:
    """Filter a dataframe for a list of supplied ICD codes"""
    if icd_version != 0:
        icd_filter = icds.loc[icds["icd_version"] == icd_version]
    else:
        icd_filter = icds
    icd_filter = icd_filter.loc[icd_filter["icd_code"].str.contains(
        '|'.join(icd_filter_list))]
    return icd_filter


def filter_drg_df(hf_drg: pd.DataFrame, drg_filter_list: List[str]) -> pd.DataFrame:
    """Filter a dataframe for a list of supplied DRG codes"""
    hf_filter = hf_drg.loc[hf_drg["drg_code"].isin(
        drg_filter_list)]
    return hf_filter


def build_sql_query(table_module: str, table_name: str, id_type: str) -> str:
    """Generates sql query"""
    sql_query = 'select ' + table_module + '.' + table_name + '.* \
                       from ' + table_module + '.' + table_name + ' join (values {0}) \
                       as to_join(' + id_type + ') \
                       ON ' + table_module + '.' + table_name + '.' + id_type + ' \
                       = to_join.' + id_type
    return sql_query


def extract_ed_table_for_ed_stays(db_cursor, ed_stays: List, table_name: str) -> pd.DataFrame:
    """Extract emergency department table for a list of ed stays"""
    sql_id_list = prepare_id_list_for_sql(ed_stays)
    sql_query = build_sql_query("mimic_ed", table_name, "stay_id")
    db_cursor.execute(sql_query.format(sql_id_list))
    ed_table = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    ed_table = pd.DataFrame(ed_table, columns=cols)
    return ed_table


def extract_emergency_department_stays_for_admission_ids(db_cursor, hospital_admission_ids: List
                                                         ) -> pd.DataFrame:
    """Extract ed stays for a list of hospital admission ids"""
    sql_id_list = prepare_id_list_for_sql(hospital_admission_ids)
    sql_query = build_sql_query("mimic_ed", "edstays", "hadm_id")
    db_cursor.execute(sql_query.format(sql_id_list))
    ed_stays = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    ed_stays = pd.DataFrame(ed_stays, columns=cols)
    return ed_stays


def extract_admissions_for_admission_ids(db_cursor, hospital_admission_ids: List) -> pd.DataFrame:
    """Extract admissions for a list of hospital admission ids"""
    sql_id_list = prepare_id_list_for_sql(hospital_admission_ids)
    sql_query = build_sql_query("mimic_core", "admissions", "hadm_id")
    db_cursor.execute(sql_query.format(sql_id_list))
    adm = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    adm = pd.DataFrame(adm, columns=cols)
    return adm


def extract_transfers_for_admission_ids(db_cursor, hospital_admission_ids: List) -> pd.DataFrame:
    """Extract transfers for a list of hospital admission ids"""
    sql_id_list = prepare_id_list_for_sql(hospital_admission_ids)
    sql_query = build_sql_query("mimic_core", "transfers", "hadm_id")
    db_cursor.execute(sql_query.format(sql_id_list))
    transfers = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    transfers = pd.DataFrame(transfers, columns=cols)
    return transfers


def extract_poe_for_admission_ids(db_cursor, hospital_admission_ids: List) -> pd.DataFrame:
    """Extract provider order entries for a list of hospital admission ids"""
    sql_id_list = prepare_id_list_for_sql(hospital_admission_ids)
    sql_query = build_sql_query("mimic_hosp", "poe", "hadm_id")
    db_cursor.execute(sql_query.format(sql_id_list))
    poe = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    poe = pd.DataFrame(poe, columns=cols)
    db_cursor.execute(
        'SELECT * FROM mimic_hosp.poe_detail')
    poe_d = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    poe_d = pd.DataFrame(poe_d, columns=cols)
    poe_d = poe_d.drop_duplicates(
        "poe_id")[["poe_id", "field_name", "field_value"]]
    poe = poe.merge(poe_d, how="left", on="poe_id")
    return poe


def extract_table_for_admission_ids(db_cursor, hospital_admission_ids: List,
                                    mimic_module: str, table_name: str) -> pd.DataFrame:
    """Extract any table in MIMIC for a list of hospital admission ids"""
    sql_id_list = prepare_id_list_for_sql(hospital_admission_ids)
    sql_query = build_sql_query(mimic_module, table_name, "hadm_id")
    db_cursor.execute(sql_query.format(sql_id_list))
    table = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    table = pd.DataFrame(table, columns=cols)
    return table

def extract_table_for_subject_ids(db_cursor, hospital_subject_ids: List,
                                    mimic_module: str, table_name: str) -> pd.DataFrame:
    """Extract any table in MIMIC for a list of hospital admission ids"""
    sql_id_list = prepare_id_list_for_sql(hospital_subject_ids)
    sql_query = build_sql_query(mimic_module, table_name, "subject_id")
    db_cursor.execute(sql_query.format(sql_id_list))
    table = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    table = pd.DataFrame(table, columns=cols)
    return table


def extract_table(db_cursor, mimic_module: str, table_name: str) -> pd.DataFrame:
    """Extract any table in MIMIC for a list of hospital admission ids"""
    db_cursor.execute(
        'SELECT * FROM ' + mimic_module + '.' + table_name)
    table = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    table = pd.DataFrame(table, columns=cols)
    return table


def extract_table_columns(db_cursor, mimic_module: str, table_name: str) -> List[str]:
    """Extract columns from a table"""
    db_cursor.execute(
        'SELECT * FROM ' + mimic_module + '.' + table_name + ' where 1=0')
    cols = list(map(lambda x: x[0], db_cursor.description))
    return cols


def get_table_module(table_name: str) -> str:
    """Provides module for a given table name"""
    if table_name in core_tables:
        module = "mimic_core"
    elif table_name in hosp_tables:
        module = "mimic_hosp"
    elif table_name in icu_tables:
        module = "mimic_icu"
    elif table_name in ed_tables:
        module = "mimic_ed"

    return module


def prepare_id_list_for_sql(id_list: List) -> str:
    """Prepares a list of ids for the sql statement"""
    id_list = [str(i) for i in id_list]
    sql_list = '), ('.join(['', *id_list, ''])
    sql_list = sql_list[3:]
    sql_list = sql_list[:len(sql_list)-3]
    return sql_list


def get_filename_string(file_name: str, file_ending: str) -> str:
    """Creates a filename string containing the creation date"""
    date = datetime.now().strftime("%d-%m-%Y-%H_%M_%S")
    return file_name + "_" + date + file_ending


def join_event_attributes_with_log_events(log: pd.DataFrame, event_attributes: pd.DataFrame,  # pylint: disable=unused-argument, line-too-long  \
                                          case_notion: str, time_column: str, start_column: str,\
                                          end_column: str) -> pd.DataFrame:
    """Joins event attribute events with events in an event log"""

    sqlcode = '''
    select *
    from event_attributes
    inner join log on event_attributes.''' + case_notion + '''=log.''' + case_notion + '''
    where event_attributes.''' + time_column + ''' >= log.''' + start_column + '''
    and event_attributes.''' + time_column + ''' <= log.''' + end_column

    joined_df = ps.sqldf(sqlcode, locals())
    joined_df = joined_df.loc[:, ~joined_df.columns.duplicated()]
    joined_df = joined_df.sort_values([case_notion, time_column])
    joined_df = joined_df.reset_index()
    joined_df = joined_df.drop("index", axis=1)

    return joined_df


subject_case_attributes = ["gender", "anchor_age",
                           "anchor_year", "anchor_year_group", "dod"]

hadm_case_attributes = ["admittime", "dischtime", "deathtime", "admission_type",
                        "admission_location", "discharge_location", "insurance",
                        "language", "marital_status", "ethnicity", "edregtime",
                        "edouttime", "hospital_expire_flag", "gender", "age",
                        "icd_code", "drg_code"]

core_tables = ["admissions", "patients", "transfers"]

hosp_tables = ["diagnoses_icd", "drgcodes", "emar", "hcpcsevents", "labevents",
               "microbiologyevents", "pharmacy", "poe", "prescriptions",
               "procedures_icd", "services"]

icu_tables = ["chartevents", "datetimeevents", "icustays", "inputevents",
              "outputevents", "procedureevents"]

ed_tables = ["diagnosis", "edstays", "medrecon table", "pyxis",
             "triage", "vitalsign", "vitalsign_hl7"]

detail_tables = {"hcpcsevents": "d_hcpcs", "diagnosis_icd": "d_icd_diagnosis",
                 "procedures_icd": "d_icd_procedures", "labevents": "d_labitems",
                 "chartevents": "d_items", "datetimeevents": "d_items", "inputevents": "d_items",
                 "outputevents": "d_items", "procedureevents": "d_items", "poe": "poe_detail",
                 "pharmacy": "prescriptions", "emar":"emar_detail"}

detail_foreign_keys = {"d_hcpcs": "code", "d_icd_diagnosis": ["icd_code", "icd_version"],
                       "d_icd_procedures": ["icd_code", "icd_version"],
                       "d_labitems": "itemid", "d_items": "itemid",
                       "poe_detail": ["poe_id", "poe_seq", "subject_id"],
                       "prescriptions": "pharmacy_id", "emar_detail":"emar_id"}

illicit_tables = ["d_hcpcs", "d_icd_diagnoses", "d_icd_procedures",
                  "d_labitems", "d_items", "emar_detail", "poe_detail", "edstays",
                  "icustays"]
