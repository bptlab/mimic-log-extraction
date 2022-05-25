"""
Provides helper methods for extraction of data frames from Mimic
"""
import logging
from typing import List
from datetime import datetime
import re
import pandas as pd
import pandasql as ps
from psycopg2.extensions import cursor


logger = logging.getLogger('cli')


def extract_icd_descriptions(db_cursor: cursor) -> pd.DataFrame:
    """Extract ICD Codes and descriptions"""
    db_cursor.execute("SELECT * FROM mimic_hosp.d_icd_diagnoses")
    desc_icd = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    desc_icd_df = pd.DataFrame(desc_icd, columns=cols)
    desc_icd_df = desc_icd_df[["icd_code", "long_title"]]
    return desc_icd_df


def extract_icds(db_cursor: cursor) -> pd.DataFrame:
    """Extract ICD Codes"""
    db_cursor.execute('SELECT * FROM mimic_hosp.diagnoses_icd')
    icds = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    return pd.DataFrame(icds, columns=cols)


def extract_drgs(db_cursor: cursor) -> pd.DataFrame:
    """Extract DRG Codes"""
    db_cursor.execute("SELECT * from mimic_hosp.drgcodes")
    drgs = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    return pd.DataFrame(drgs, columns=cols)


def extract_admissions(db_cursor: cursor) -> pd.DataFrame:
    """Extract admissions"""
    db_cursor.execute('SELECT * FROM mimic_core.admissions')
    adm = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    return pd.DataFrame(adm, columns=cols)


def extract_patients(db_cursor: cursor) -> pd.DataFrame:
    """Extract patients"""
    db_cursor.execute("SELECT * from mimic_core.patients")
    patients = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    return pd.DataFrame(patients, columns=cols)


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
    icds["icd_code"] = icds["icd_code"].str.replace(" ", "") # type: ignore
    if icd_version != 0:
        icd_filter = icds.loc[icds["icd_version"] == icd_version]
    else:
        icd_filter = icds

    cond_list = []

    for icd_filter_element in icd_filter_list:
        icd_filter_element = str(icd_filter_element)
        if ":" in icd_filter_element:
            first = icd_filter_element.split(":")[0]
            second = icd_filter_element.split(":")[1]
            char = re.search('[a-zA-Z]', first)
            if char is None:
                #icd 9 - if len of string < 3 fill up with zeroes
                for i in range(int(first), int(second)+1):
                    length = len(str(i))
                    string = str(i)
                    if length == 1:
                        string = "00" + string
                    elif length == 2:
                        string = "0" + string
                    cond_list.append(string)
            else:
                #icd 10 - if len of string < 2 fill up with zeroes
                char = char[0] # type: ignore
                first = first[1:]
                second = second[1:]
                for i in range(int(first), int(second)+1):
                    length = len(str(i))
                    string = str(i)
                    if length < 2:
                        string = "0" + string
                    string = char + string # type: ignore
                    cond_list.append(string)
        else:
            cond_list.append(icd_filter_element)

    cond_list = tuple(cond_list) # type: ignore

    icd_filter = icd_filter.loc[icd_filter["icd_code"].str.startswith(cond_list, # type: ignore
                                na=False)] # type: ignore

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


def extract_ed_table_for_ed_stays(db_cursor: cursor, ed_stays: List,
                                  table_name: str) -> pd.DataFrame:
    """Extract emergency department table for a list of ed stays"""
    sql_id_list = prepare_id_list_for_sql(ed_stays)
    sql_query = build_sql_query("mimic_ed", table_name, "stay_id")
    db_cursor.execute(sql_query.format(sql_id_list))
    ed_table = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    return pd.DataFrame(ed_table, columns=cols)


def extract_emergency_department_stays_for_admission_ids(db_cursor: cursor,
                                                         hospital_admission_ids: List
                                                         ) -> pd.DataFrame:
    """Extract ed stays for a list of hospital admission ids"""
    sql_id_list = prepare_id_list_for_sql(hospital_admission_ids)
    sql_query = build_sql_query("mimic_ed", "edstays", "hadm_id")
    db_cursor.execute(sql_query.format(sql_id_list))
    ed_stays = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    return pd.DataFrame(ed_stays, columns=cols)


def extract_admissions_for_admission_ids(db_cursor: cursor,
                                         hospital_admission_ids: List) -> pd.DataFrame:
    """Extract admissions for a list of hospital admission ids"""
    sql_id_list = prepare_id_list_for_sql(hospital_admission_ids)
    sql_query = build_sql_query("mimic_core", "admissions", "hadm_id")
    db_cursor.execute(sql_query.format(sql_id_list))
    adm = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    return pd.DataFrame(adm, columns=cols)


def extract_transfers_for_admission_ids(db_cursor: cursor,
                                        hospital_admission_ids: List) -> pd.DataFrame:
    """Extract transfers for a list of hospital admission ids"""
    sql_id_list = prepare_id_list_for_sql(hospital_admission_ids)
    sql_query = build_sql_query("mimic_core", "transfers", "hadm_id")
    db_cursor.execute(sql_query.format(sql_id_list))
    transfers = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    return pd.DataFrame(transfers, columns=cols)


def extract_poe_for_admission_ids(db_cursor: cursor,
                                  hospital_admission_ids: List) -> pd.DataFrame:
    """Extract provider order entries for a list of hospital admission ids"""
    sql_id_list = prepare_id_list_for_sql(hospital_admission_ids)
    sql_query = build_sql_query("mimic_hosp", "poe", "hadm_id")
    db_cursor.execute(sql_query.format(sql_id_list))
    poe = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    poe_df = pd.DataFrame(poe, columns=cols)
    db_cursor.execute(
        'SELECT * FROM mimic_hosp.poe_detail')
    poe_d = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    poe_d_df = pd.DataFrame(poe_d, columns=cols)
    poe_d_df = poe_d_df.drop_duplicates(
        "poe_id")[["poe_id", "field_name", "field_value"]]
    poe_df = poe_df.merge(poe_d_df, how="left", on="poe_id")
    return poe_df


def extract_table_for_admission_ids(db_cursor: cursor, hospital_admission_ids: List,
                                    mimic_module: str, table_name: str) -> pd.DataFrame:
    """Extract any table in MIMIC for a list of hospital admission ids"""
    sql_id_list = prepare_id_list_for_sql(hospital_admission_ids)
    sql_query = build_sql_query(mimic_module, table_name, "hadm_id")
    db_cursor.execute(sql_query.format(sql_id_list))
    table = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    return pd.DataFrame(table, columns=cols)


def extract_table_for_subject_ids(db_cursor: cursor, hospital_subject_ids: List,
                                  mimic_module: str, table_name: str) -> pd.DataFrame:
    """Extract any table in MIMIC for a list of hospital admission ids"""
    sql_id_list = prepare_id_list_for_sql(hospital_subject_ids)
    sql_query = build_sql_query(mimic_module, table_name, "subject_id")
    db_cursor.execute(sql_query.format(sql_id_list))
    table = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    return pd.DataFrame(table, columns=cols)


def extract_table(db_cursor: cursor, mimic_module: str, table_name: str) -> pd.DataFrame:
    """Extract any table in MIMIC for a list of hospital admission ids"""
    db_cursor.execute(
        'SELECT * FROM ' + mimic_module + '.' + table_name)
    table = db_cursor.fetchall()
    cols = list(map(lambda x: x[0], db_cursor.description))
    return pd.DataFrame(table, columns=cols)


def extract_table_columns(db_cursor: cursor, mimic_module: str, table_name: str) -> List[str]:
    """Extract columns from a table"""
    db_cursor.execute(
        'SELECT * FROM ' + mimic_module + '.' + table_name + ' where 1=0')
    cols = list(map(lambda x: x[0], db_cursor.description))
    return cols

def extract_icustay_events(db_cursor: cursor, cohort: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts icustay events for a given cohort
    """

    hospital_admission_ids = list(cohort["hadm_id"].unique())
    hospital_admission_ids = [float(i) for i in hospital_admission_ids]

    icu_stays = extract_table_for_admission_ids(db_cursor, hospital_admission_ids,\
                                                "mimic_icu", "icustays")

    event_dict = {}
    i = 0
    for _, row in icu_stays.iterrows():
        column_labels = ["intime", "outtime"]
        for col in icu_stays.columns:
            if col in column_labels:
                if pd.isna(row[col]):
                    continue
                if col == "intime":
                    activity = "ICU in"
                elif col == "outtime":
                    activity = "ICU out"
                new_row = {"hadm_id": row["hadm_id"], "subject_id": row["subject_id"],
                           "activity": activity, "timestamp": row[col]}
                event_dict[i] = new_row
                i = i + 1

    log = pd.DataFrame.from_dict(event_dict, "index")  # type: ignore
    log = log.sort_values("timestamp")
    log = log.reset_index().drop("index", axis=1)
    log = log.rename({"activity": "concept:name",
                     "timestamp": "time:timestamp"}, axis=1)

    return log

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
                 "pharmacy": "prescriptions", "emar": "emar_detail"}

detail_foreign_keys = {"d_hcpcs": ["code", "short_description"],
                       "d_icd_diagnosis": ["icd_code", "icd_version"],
                       "d_icd_procedures": ["icd_code", "icd_version"],
                       "d_labitems": "itemid", "d_items": "itemid",
                       "poe_detail": ["poe_id", "poe_seq", "subject_id"],
                       "prescriptions": "pharmacy_id", "emar_detail": "emar_id"}

illicit_tables = ["d_hcpcs", "d_icd_diagnoses", "d_icd_procedures",
                  "d_labitems", "d_items", "emar_detail", "poe_detail", "edstays"]
