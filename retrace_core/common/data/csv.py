from typing import Union

import warnings
import pandas as pd
from .errors import requiredColumns, RequiredColumnError, suggestedColumns, SuggestedColumnWarning

from pm4py.objects.log.util import dataframe_utils

DataFrame_or_Series = Union[pd.Series, pd.DataFrame]


def read_csv(path: str, col_map: dict = {}) -> pd.DataFrame:
    """ Reads the log.

    Reads a .csv file into a pandas dataframe and applies
    the necessary transformations.

    Args:
        path (str):
            The path to the log file.
        col_map (dict):
            Pandas column rename mapping. Default is {}, don"t rename any columns.

    Returns:
        pd.DataFrame: The transition log as a pandas dataframe.
    """
    log_csv = pd.read_csv(path, low_memory=False)
    log_csv.rename(columns=col_map, inplace=True)

    # Checks if required columns are missing
    for column, description in requiredColumns.items():
        if column not in log_csv.columns:
            raise RequiredColumnError(path, col_map, column)

    # Check if suggested columns are missing
    for column, description in suggestedColumns.items():
        if column not in log_csv.columns:
            warnings.warn(SuggestedColumnWarning.warn(column, description), SuggestedColumnWarning)

    log_csv = dataframe_utils.convert_timestamp_columns_in_df(
        log_csv, "%Y-%m-%d %H:%M:%S", "time:timestamp")

    if "time:timestamp:start" in log_csv.columns:
        log_csv = dataframe_utils.convert_timestamp_columns_in_df(
            log_csv, "%Y-%m-%d %H:%M:%S", "time:timestamp:start")

        log_csv = log_csv.sort_values(["time:timestamp:start", "time:timestamp"])
    else:
        log_csv = log_csv.sort_values(["time:timestamp"])

    return log_csv
