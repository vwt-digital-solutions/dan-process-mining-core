from pm4py.algo.filtering.pandas.cases import case_filter


def performance_filter(log, value):
    """ Filters the log based on performance.

    Args:
        log (pd.DataFrame):
            The transition log.
        value (dict):
            The filter's value or settings.

    Returns:
        pd.DataFrame:
            The filtered transition log.
    """
    max_case_performance = value.get('maxCasePerformance')
    if max_case_performance:
        min_case_performance = value.get('minCasePerformance', 0) * 86400
        max_case_performance = max_case_performance * 86400

        log = case_filter.filter_case_performance(
            log,
            min_case_performance=min_case_performance,
            max_case_performance=max_case_performance
        )

    return log
