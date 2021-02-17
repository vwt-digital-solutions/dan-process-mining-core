from .attribute_filter import attribute_filter
from .endpoints_filter import endpoints_filter
from .performance_filter import performance_filter
from .time_filter import TimeFilter

import pandas as pd

def apply_filters(log: pd.DataFrame, request_body={}):
    """Filters the transition log based on the settings in the request body.

    Args:
        log (pd.DataFrame):
            The unfiltered transition log.
        request_body (dict, optional):
            The request body that was sent to the API. Defaults to {}.

    Returns:
        pd.DataFrame:
            The filtered transition log.
    """

    filters = request_body.get('filters', {})
    filterFunctions = {
        'Timeframe': TimeFilter.apply,
        'Performance': performance_filter,
        'Attribute': attribute_filter,
        'Endpoints': endpoints_filter,
    }

    if not len(filters):
        return log

    for filter_obj in filters:
        value = filter_obj.get('value', {})
        filterFunctions[filter_obj['name']](log, value)

    return log
