from retrace_core.common.data import csv


def from_csv(path: str, colmap: dict = {}):
    log = csv.read_csv(path, col_map=colmap)

    return log
