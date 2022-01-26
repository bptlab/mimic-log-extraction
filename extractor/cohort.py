def extract_cohort(db_cursor, icd_codes, drg_codes, ages):
    # select patients filtered by age
    # join on filtered icd and drg

    if ages is None or ages == ['']:
        # select all patients
        return None
    else:
        #filter patients
        print("filter")


    return None
