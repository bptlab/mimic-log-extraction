"""Defines constants for input/output and database interaction"""

INCLUDE_MEDICATION_QUESTION = """POE links to medication tables
(pharmacy, emar, prescriptions).\n Shall the medication events be enhanced by the 
concrete medications prescribed? (Y/N):"""

ADDITIONAL_ATTRIBUTES_QUESTION = """Shall the event log be enhanced by additional event
attributes from other tables in the database? (Y/N):"""

SUBJECT_CASE_NOTION = "SUBJECT"
SUBJECT_CASE_KEY = 'subject_id'

ADMISSION_CASE_NOTION = 'HOSPITAL ADMISSION'
ADMISSION_CASE_KEY = 'hadm_id'

ADMISSION_EVENT_TYPE = "ADMISSION"
TRANSFER_EVENT_TYPE = "TRANSFER"
POE_EVENT_TYPE = "POE"
OTHER_EVENT_TYPE = "OTHER"
