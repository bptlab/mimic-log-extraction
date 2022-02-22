# mimic-log-extraction-tool

- requires python 3.8.10 (newer versions might be fine, though)
- using a python virtual environment seems like a good idea

## usage

```
usage: extract_log.py [-h] [--db_name DB_NAME] [--db_host DB_HOST] [--db_user DB_USER] [--db_pw DB_PW] [--icd ICD] [--drg DRG] [--age AGE] [--type TYPE]

optional arguments:
  -h, --help         show this help message and exit
  --db_name DB_NAME  Database Name
  --db_host DB_HOST  Database Host
  --db_user DB_USER  Database User
  --db_pw DB_PW      Database Password
  --icd ICD          ICD code(s) of cohort
  --drg DRG          DRG code(s) of cohort
  --age AGE          Patient Age of cohort
  --type TYPE        Event Type
```

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
