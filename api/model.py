import os

# PM4PY
from pm4py.objects.dfg.retrieval.pandas import get_dfg_graph

# Local imports
from .filters.main import apply_filters
from .utils import export_process_map, normalize, read_log

data_path = os.getenv("DATA_PATH")


def create_dfg_network(request_body: dict) -> dict:
    """ Creates a direct-follows graph.

    Args:
        request_body:
            The HTTP request body

    Returns:
        dict: A dictionary containing the dfg export.
    """
    log_df = read_log(f'{data_path}/{request_body.get("file")}')
    log_df = apply_filters(log_df, request_body)

    extra_args = {}
    node_performance = {}
    if not log_df["time:timestamp:start"].isnull().any():
        # Add start timestamp to get_dfg_graph arguments
        extra_args["start_timestamp_key"] = "time:timestamp:start"

        # Manually calculate time spent on a task (activity time)
        log_df["timeDiff"] = \
            (log_df["time:timestamp"] - log_df["time:timestamp:start"]).dt.seconds

        time_spent_on_task = log_df.groupby("concept:name").agg({
            "timeDiff": ["mean", "max", "min"]
        })

        # Remove the unused multi-index.
        time_spent_on_task.columns = time_spent_on_task.columns.droplevel(0)

        # Calculate the normalized versions of the performance so that the frontend may color them using viridis colormap
        time_spent_on_task['norm_mean'] = normalize(time_spent_on_task['mean'])
        time_spent_on_task['norm_min'] = normalize(time_spent_on_task['min'])
        time_spent_on_task['norm_max'] = normalize(time_spent_on_task['max'])

        # Convert the pandas dataframe to a dictionary.
        node_performance = time_spent_on_task.to_dict()

    # Metrics
    rel_freq_dfg = get_dfg_graph(
        log_df, measure="frequency", keep_once_per_case=True, **extra_args)
    abs_freq_dfg, perf_mean_dfg = get_dfg_graph(
        log_df, measure="both", perf_aggregation_key="mean", **extra_args)
    perf_max_dfg = get_dfg_graph(
        log_df, measure="performance", perf_aggregation_key="max", **extra_args)
    perf_min_dfg = get_dfg_graph(
        log_df, measure="performance", perf_aggregation_key="min", **extra_args)
    perf_median_dfg = get_dfg_graph(
        log_df, measure="performance", perf_aggregation_key="median", **extra_args)

    return {
        "network": export_process_map({
            "nodes": {
                "performance": node_performance
            },
            "edges": {
                "performance": {
                    "mean": perf_mean_dfg,
                    "min": perf_min_dfg,
                    "max": perf_max_dfg,
                    "median": perf_median_dfg
                },
                "frequency": {
                    "relative": rel_freq_dfg,
                    "absolute": abs_freq_dfg
                }
            }
        }, request_body)
    }
