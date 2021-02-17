from typing import Union, Optional

import pandas as pd

from pm4py.objects.log.util import dataframe_utils

DataFrame_or_Series = Union[pd.Series, pd.DataFrame]

def read_log(path: str, rename_cols: bool = True) -> pd.DataFrame:
    """ Reads the log.

    Reads a .csv file into a pandas dataframe and applies
    the necessary transformations.

    Args:
        path (str):
            The path to the log file.
        rename_cols (bool):
            Whether to rename the columns. Default is True.
    
    Returns:
        pd.DataFrame: The transition log as a pandas dataframe.
    """
    log_csv = pd.read_csv(path, low_memory=False)

    # Only select substatus type
    log_csv = log_csv[log_csv["Type"] == "Substatus"]

    if rename_cols:
        log_csv.rename(columns={
            "Naam": "concept:name",  # Activity name
            #         "Projectnummer": "case:clientID",
            "Eindtijd": "time:timestamp",  # Endtime
            "Starttijd": "time:timestamp:start",  # Starttime (optional)
            "ProjectNummer": "case:concept:name",  # Project number
            "Medewerker": "org:resource"  # Resource that performed transition. (optional)
        }, inplace=True)

    log_csv = dataframe_utils.convert_timestamp_columns_in_df(
        log_csv, "%Y-%m-%d %H:%M:%S", "time:timestamp")

    log_csv = dataframe_utils.convert_timestamp_columns_in_df(
        log_csv, "%Y-%m-%d %H:%M:%S", "time:timestamp:start")

    log_csv = log_csv.sort_values(["time:timestamp:start", "time:timestamp"])

    return log_csv


def create_edge(source: str, target: str, metrics: dict) -> dict:
    """Creates an edge from source to target.

    Args:
        source (str):
            The source node id.
        target (str):
            The target node id.
        metrics (dict):
            A dictionary containing the metrics for the edge.

    Returns:
        dict:
            The created edge.
    """

    edge = {
        "source": source,
        "target": target,
        "metrics": metrics
    }

    # If a node leads to itself set the edge type to "loop"
    if source == target:
        edge["type"] = "multipleLabelsEdgeLoop"

    return edge


def export_process_map(dfg_dict: dict, request_body: dict = {}):
    """Creates a process map from multiple DFG's.

    Args:
        dfg_dict (dict):
            A dictionary of directly-follows-graphs.
        request_body (dict):
            The HTTP request body

    Returns:
        dict: A dictionary containing the DFG.
    """
    node_id_set = set()

    data = {
        "nodes": [],  # Nodes in our network
        "edges": [],  # Connections in our network
    }

    controls = request_body.get("controls", {})
    min_edge_occurences = controls.get("min_edge_occurences", 1)

    for key, frequency in dfg_dict["edges"]["frequency"]["absolute"].items():
        if (frequency < min_edge_occurences):
            continue

        source = key[0]
        target = key[1]

        # Add node
        if (source not in node_id_set):
            metric = None
            if (dfg_dict['nodes']['performance']):
                metric = get_node_metric(
                    metric=dfg_dict['nodes']['performance'],
                    source=source
                )

            node = {
                "id": source,
                "label": source,
                "data": {
                    "performance": metric
                }
            }

            data["nodes"].append(node)
            node_id_set.add(node["id"])

        # Get edge metrics
        performance = dfg_dict["edges"].get("performance")
        frequency_metrics = dfg_dict["edges"].get("frequency")

        # Create an edge, the label is always the absolute value or the mean if multiple values exist.
        edge = create_edge(source, target, {
            "frequency": {
                "absolute": frequency_metrics["absolute"].get((source, target), 0),
                "relative": frequency_metrics["relative"].get((source, target), 0),
            },
            "performance": {
                "mean": performance["mean"].get((source, target), 0),
                "min": performance["min"].get((source, target), 0),
                "max": performance["max"].get((source, target), 0),
                "median": performance["median"].get((source, target), 0),
            }
        })

        data["edges"].append(edge)

    return data


def get_node_metric(metric: dict, source: str) -> Optional[dict]:
    """Retrieves a metric that belongs to a node (activity).

    Args:
        metric (dict):
            The metrics dictionary for all nodes.

            The dict contains the following 6 keys:
            - mean:
                Mean of the metric (could be mean performance, mean frequency, etc)
            - max
                Maximum of the metric
            - min
                Minimum of the metric
            - [norm_mean, norm_max, norm_min]
                The same value as non-norm equivalent except normalized between 0 and 1.

        source (str):
            The node id.

    Returns:
        Optional[dict]:
        A dictionary containing the metrics belonging to this node.

    Example:
        >>> x = { 'mean': { 'node-1': 200, 'node-2': 100, }, ... }
        >>> get_node_metric(x, "node-1")
        { 
            "mean": 200,
            ... 
        }
    """
    if not metric:
        return None

    return {
        "mean": metric["mean"].get(source),
        "min": metric["min"].get(source),
        "max": metric["max"].get(source),
        "norm_mean": metric["norm_mean"].get(source),
        "norm_min": metric["norm_min"].get(source),
        "norm_max": metric["norm_max"].get(source),
    }


def normalize(table: DataFrame_or_Series) -> DataFrame_or_Series:
    """Normalizes a pandas series or dataframe.

    Normalizes a pandas dataframe or series based on min-max normalization.

    Args:
        table (DataFrame_or_Series):
            The pandas series or dataframe to normalize.

    Returns:
        DataFrame_or_Series:
            The normalized pandas dataframe or series.

    Example:
        >>> normalize(pd.Series([0, 50, 100]))
        0    0.0
        1    0.5
        2    1.0
        dtype: float64

        >>> normalize(pd.DataFrame(data={'col1': [1,2,3,4], 'col2': [0,50,75,100]}))
               col1  col2
        0  0.000000  0.00
        1  0.333333  0.50
        2  0.666667  0.75
        3  1.000000  1.00

    """
    return (table - table.min()) / (table.max() - table.min())
