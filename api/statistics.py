import os

import numpy as np

# PM4PY
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.statistics.traces.common import case_duration as case_duration_commons
from pm4py.statistics.traces.log import case_statistics
from pm4py.statistics.traces.parameters import Parameters

SECONDS_IN_DAY = 86400
data_path = os.getenv("DATA_PATH")


def case_durations(log_csv, request_body={}):
    """ Calculate the duration of all cases.

    Args:
        log_csv (pd.DataFrame):
            The transitionlog dataframe.
        request_body:
            The HTTP request body.

    Returns:
        (dict): A dictionary with the histogram and line data.
    """

    BINS_COUNT = 50

    if len(log_csv['WF_nr'].unique()) <= 1:
        return

    log = log_converter.apply(log_csv, variant=log_converter.Variants.TO_EVENT_LOG)

    # TODO: add interval log functionality
    durations = case_statistics.get_cases_description(log, parameters={
        case_statistics.Parameters.BUSINESS_HOURS: True,
        case_statistics.Parameters.WORKTIMING: request_body.get('workhours', [9, 17]),
    })

    # Convert seconds to days
    duration_vals = [
        x["caseDuration"] / SECONDS_IN_DAY for x in durations.values()
    ]

    # Get the kde lineplot
    x, y = case_duration_commons.get_kde_caseduration(duration_vals, parameters={
        Parameters.GRAPH_POINTS: BINS_COUNT * 4  # The default is 200
    })

    # Get the histogram
    density, bins = np.histogram(duration_vals, bins=BINS_COUNT, density=1)

    return {
        'histogram': {
            'density': list(density),
            'bins': list(bins)
        },
        'line': {
            'x': list(x),
            'y': list(y)
        }
    }
