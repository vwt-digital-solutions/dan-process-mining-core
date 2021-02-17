import pandas as pd

from pm4py.algo.filtering.pandas.attributes import attributes_filter


def attribute_filter(log: pd.DataFrame, value: dict) -> pd.DataFrame:
    """ Filters the log based on a column's attribute(s).

    Args:
        log (pd.DataFrame):
            The transition log.
        value (dict):
            The value/settings of the filter.

    Returns:
        pd.DataFrame:
            The filtered transition log.
    """
    values = value.get('selected')
    enable_inverse = value.get('inverse', False)
    filter_column = value.get('filterColumn')
    mode = value.get('mode', 'contain')

    functions = {
        'contain': attributes_filter.apply,
        'trim': attributes_filter.apply_events
    }

    if (not values or not filter_column):
        print('Please provide a column and values to the attribute filter')
        print('Values:', values)
        print('Column:', filter_column)
    else:
        log = functions[mode](
            log,
            values,
            parameters={
                attributes_filter.Parameters.ATTRIBUTE_KEY: filter_column,
                attributes_filter.Parameters.POSITIVE: not enable_inverse
            }
        )

    return log
