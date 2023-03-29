# mimic-log-extraction

[![Pylint](https://github.com/bptlab/mimic-log-extraction/actions/workflows/pylint.yml/badge.svg)](https://github.com/bptlab/mimic-log-extraction/actions/workflows/pylint.yml) [![Typecheck](https://github.com/bptlab/mimic-log-extraction/actions/workflows/mypy.yml/badge.svg)](https://github.com/bptlab/mimic-log-extraction/actions/workflows/mypy.yml)

A CLI tool for extracting event logs out of MIMIC Databases. This branch is for MIMIC-IV 2.2. If you use MIMIC-IV 2.0, please pull from the respective branch: https://github.com/bptlab/mimic-log-extraction/tree/mimic-2.0

- requires python 3.8.10 (newer versions might be fine, though)
- using a python virtual environment seems like a good idea

The official python documentation provides a [good overview](https://docs.python.org/3/library/venv.html) on how to create virtual environments. We recommend having the environment either in this directory, or one level above.

## usage

```
usage: extract_log.py [-h] [--db_name DB_NAME] [--db_host DB_HOST] [--db_user DB_USER] [--db_pw DB_PW] [--subject_ids SUBJECT_IDS]
                      [--hadm_ids HADM_IDS] [--icd ICD] [--icd_version ICD_VERSION] [--icd_sequence_number ICD_SEQUENCE_NUMBER] [--drg DRG]
                      [--drg_type DRG_TYPE] [--age AGE] [--type TYPE] [--tables TABLES] [--tables_activities TABLES_ACTIVITIES]
                      [--tables_timestamps TABLES_TIMESTAMPS] [--notion NOTION] [--case_attribute_list CASE_ATTRIBUTE_LIST] [--config CONFIG]
                      [--save_intermediate] [--ignore_intermediate]

optional arguments:
  -h, --help            show this help message and exit
  --db_name DB_NAME     Database Name
  --db_host DB_HOST     Database Host
  --db_user DB_USER     Database User
  --db_pw DB_PW         Database Password
  --subject_ids SUBJECT_IDS
                        Subject IDs of cohort
  --hadm_ids HADM_IDS   Hospital Admission IDs of cohort
  --icd ICD             ICD code(s) of cohort
  --icd_codes_intersection Optional argument, if one wants to filter for disease combinations, such that patients have to have an icd code from icd_codes and from icd_codes_intersection
  --icd_version ICD_VERSION
                        ICD version
  --icd_sequence_number ICD_SEQUENCE_NUMBER
                        Ranking threshold of diagnosis
  --drg DRG             DRG code(s) of cohort
  --drg_type DRG_TYPE   DRG type (HCFA, APR)
  --age AGE             Patient Age of cohort
  --type TYPE           Event Type
  --tables TABLES       Low level tables
  --tables_activities TABLES_ACTIVITIES
                        Activity Columns for Low level tables
  --tables_timestamps TABLES_TIMESTAMPS
                        Timestamp Columns for Low level tables
  --notion NOTION       Case Notion
  --case_attribute_list CASE_ATTRIBUTE_LIST
                        Case Attributes
  --config CONFIG       Config file for providing all options via file
  --save_intermediate   Store intermediate extraction results as csv. For debugging purposes.
  --ignore_intermediate
                        Explicitly disable storing of intermediate results.
  --csv_log             Store resulting log as a .csv file instead of as an .xes event log
```

Call the tool via

```bash
python3 -m extract_log <...>
```

passing the required parameters.

If you installed the tool via cloning this repository, you should instead execute

```bash
python3 ./extract_log.py <...>
```

## config file

For providing parameters via a `.yml` config file, provide the path to that file via the `--config` flag.
This will override any setting provided via prompt or input flag, so be careful. Refer to the `example_config.yml` file for how to provide options. The config keys `icd_codes`, `drg_codes`, and `additional_event_attributes` need to be explicitly set to `[]` in order to not be prompted for during extraction. `include_medications` only needs to be set for POE event logs to avoid the prompt. When `case_attributes` is set to `[]`, the respective default attributes are used. If the key is not provided, no case attributes are added. To be prompted for it during execution, `prompt_case_attributes` needs to be set to true.

```yaml
db:
    name: mimic
    host: 127.0.0.1
    user: some_db_user
    pw: some_db_password
save_intermediate: True # True, False
csv_log: False # True, defaults to False
cohort:
    subject_ids: # Omitting does not consider subject_ids
        - some subject_ids
        - ...
    hadm_ids: # Omitting does not consider hadm_ids
        - some hadm_ids
        - ...
    icd_codes: # could also be [] to avoid ICD filtering. Omitting makes the tool prompt for input.
        - some ICD code
        - ...
    icd_codes_intersection: # optional argument, if one wants to filter for disease combinations, such that patients have to have an icd code from icd_codes and from icd_codes_intersection
        - some ICD code
        - ...   
    icd_version: 10 # 9, 10, 0
    icd_seq_num: 1
    drg_codes: [] # could also contain keys to filter for DRG codes. Omitting makes the tool prompt for input. 
    drg_ontology: APR # APR, HCFA
    age: # could also be [] to avoid age range filtering. Omitting makes the tool prompt for input.
        - 0:25
        - 50:90
event_type: admission # admission, transfer, poe
include_medications: False # False, True. Only needed if POE event_type
case_notion: hospital admission # subject, hospital admission
case_attributes: [] # could also be None. [] uses default case attributes for case notion.
prompt_case_attributes: False # False, True. Setting True forces case attributes to be determined if not provided
low_level_tables: # only if event type OTHER
    - pharmacy
    - labevents
low_level_activities:
    - medication
    - label
low_level_timestamps:
    - starttime
    - charttime
additional_event_attributes: # Can be set to []. Omitting makes the tool prompt for input
    - 
        start_column: a
        end_column: b
        time_column: c
        table_to_aggregate: d
        column_to_aggregate: f
        aggregation_method: g
        filter_column: h # can be omitted
        filter_values:
            - one
            - other
    -
        start_column: a
        end_column: b
        time_column: c
        table_to_aggregate: d
        column_to_aggregate: f
        aggregation_method: g
        filter_column: h # can be omitted
```

## installation

Simply run the pip installation command to install the extraction tool:

```bash
pip install git+https://github.com/bptlab/mimic-log-extraction/
```

Alternatively, clone this repo and execute

```bash
pip install -e .
```

For development and testing, all dev dependencies can be installed using

```bash
pip install -e .[dev]
```

If you're using `zsh`, escape the square brackets: `pip install -e .\[dev\]`

## development

After installing all required dev dependencies, make sure to regularly call

```bash
pylint extract_log.py extractor --rcfile .pylintrc
mypy --config-file mypy.ini .
```

to ensure linted and typechecked code.
