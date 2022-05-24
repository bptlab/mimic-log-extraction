# mimic-log-extraction-tool

A CLI tool for extracting event logs out of MIMIC Databases.

- requires python 3.8.10 (newer versions might be fine, though)
- using a python virtual environment seems like a good idea

## usage

```bash
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

## config file

For providing parameters via a `.yml` config file, provide the path to that file via the `--config` flag.
This will override any setting provided via prompt or input flag, so be careful. Refer to the `example_config.yml` file for how to provide options. The config keys `icd_codes`, `drg_codes`, and `additional_event_attributes` need to be explicitly set to `[]` in order to not be prompted for during extraction. `include_medications` only needs to be set for POE event logs to avoid the prompt. When `case_attributes` is set to `[]`, the respective default attributes are used. If the key is not provided, no case attributes are added. To be prompted for it during execution, `prompt_case_attributes` needs to be set to true.

## installation

Simply run the pip installation command to install the extraction tool:

```bash
pip install git+https://gitlab.hpi.de/finn.klessascheck/mimic-log-extraction-tool
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
