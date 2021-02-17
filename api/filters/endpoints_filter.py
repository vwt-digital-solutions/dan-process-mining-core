import pandas as pd

from pm4py.algo.filtering.pandas.end_activities import end_activities_filter
from pm4py.algo.filtering.pandas.start_activities import start_activities_filter


def endpoints_filter(log: pd.DataFrame, value: dict) -> pd.DataFrame:
    """ Filters a log based on the start or endpoints.

    Args:
        log (pd.DataFrame):
            The transition log.
        value (dict):
            The filter's value or settings.

    Returns:
        pd.DataFrame:
            The filtered transition log.
    """
    start_activities = value.get('startActivities')
    end_activities = value.get('endActivities')

    print(len(log))
    if start_activities:
        log = start_activities_filter.apply(log, start_activities)

    if end_activities:
        log = end_activities_filter.apply(log, end_activities)
    print(len(log))

    return log


def calc_start_end_activities(log: pd.DataFrame) -> dict:
    """Calculates the start and end activaties of the transition log.

    Args:
        log (pd.DataFrame):
            The transition log

    Returns:
        dict:
            A dict containing the start and end activities.
    """

    return {
        'end_activities': list(end_activities_filter.get_end_activities(log)),
        'start_activities': list(start_activities_filter.get_start_activities(log)),
    }
