"""Provides functionality to enhance event logs with event attributes"""
import logging
import warnings
import pandas as pd
from .helper import (join_event_attributes_with_log_events)
from .tables import (extract_tables)

warnings.filterwarnings("ignore")

logger = logging.getLogger('cli')

# todo: add type annotations in method signatures

def extract_event_attributes(db_cursor, log, start_column, end_column, time_column, \
                            table_to_aggregate, column_to_aggregate, \
                            aggregation_method, filter_column, filter_values) -> pd.DataFrame:
    """
    Extracts event attributes for a given event log
    """
    case_notion = "hadm_id"
    logger.info("Begin extracting event attributes!")

    hospital_admission_ids = list(log[case_notion].unique())
    hospital_admission_ids = [float(i) for i in hospital_admission_ids]

    event_attributes = extract_tables(db_cursor, [table_to_aggregate], \
                                      hospital_admission_ids, None)

    event_attributes = event_attributes.sort_values([case_notion, time_column])

    joined_df = join_event_attributes_with_log_events(log, event_attributes, case_notion,\
                                                     time_column, start_column, end_column)

    aggregation_dict = {}
    for col in column_to_aggregate:
        aggregation_dict[col] = aggregation_method


    if filter_column is not None:
        aggregated_df = joined_df.groupby([case_notion, filter_column, start_column, end_column])\
                                    .agg(aggregation_dict).reset_index()
    else:
        aggregated_df = joined_df.groupby([case_notion, start_column, end_column])\
                                    .agg(aggregation_dict).reset_index()

    aggregated_df[start_column] = aggregated_df[start_column].apply(pd.to_datetime)
    aggregated_df[end_column] = aggregated_df[end_column].apply(pd.to_datetime)
    log[start_column] = log[start_column].apply(pd.to_datetime)
    log[end_column] = log[end_column].apply(pd.to_datetime)

    if filter_column is not None:
        for filter_val in filter_values:
            filter_val_df = aggregated_df.loc[aggregated_df[filter_column] == filter_val]
            for col in column_to_aggregate:
                filter_val_df.rename({column_to_aggregate:filter_val + "_" + column_to_aggregate},\
                                      axis=1, inplace=True)
            filter_val_df.drop(filter_column, axis=1, inplace=True)
            log = log.merge(filter_val_df, on=[case_notion, start_column, end_column], how="left")
    else:
        log = log.merge(aggregated_df, on=[case_notion, start_column, end_column], how="left")

    logger.info("Done extracting event attributes!")

    return log
