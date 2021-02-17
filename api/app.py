import os

from dotenv import load_dotenv
import flask
from flask import jsonify, request
from flask_cors import CORS
from pm4py.algo.filtering.pandas.attributes import attributes_filter
from pm4py.statistics.traces.pandas import case_statistics

from .filters.endpoints_filter import calc_start_end_activities
from .json_encoder import CustomJSONEncoder
from .model import create_dfg_network
from .statistics import case_durations
from .utils import read_log

load_dotenv()

app = flask.Flask(__name__)
app.config["DEBUG"] = True
app.json_encoder = CustomJSONEncoder
CORS(app)

data_path = os.getenv("DATA_PATH")


@app.route('/', methods=['POST'])
def generate_network() -> dict:
    """ Generates the process map from the transition log.

    Returns:
        dict: The graph representation in json.
    """

    body = request.json

    if 'file' in body:
        network = create_dfg_network(body)
        return network


# A route that returns all the case durations
@app.route('/case_durations', methods=['POST'])
def get_case_durations() -> dict:
    """ Returns the calculated case durations.

    Returns:
        dict: histogram and line datapoints.
    """
    body = request.json

    if 'file' in body:
        log_df = read_log(f'{data_path}/{body["file"]}')
        durations = case_durations(log_df, request_body=body)
        return jsonify(durations)
    else:
        return {
            "error": "Please provide a filename"
        }


# A route to return all of the PMC files in the data directory.
@app.route('/files', methods=['GET'])
def get_files() -> list:
    """ Returns a list of filenames.

    Returns:
        list: A list of all the filenames in the data directory.
    """
    return jsonify(os.listdir(data_path))


# FIXME: remove this function if unused.
# A route to return all case variants in a log.
@app.route('/case_variants', methods=['POST'])
def get_case_variants() -> list:
    """ Returns a list of case variants.

    Returns:
        list: A list of all the case variants in the log.
    """
    body = request.json

    if 'file' in body:
        log_df = read_log(f'{data_path}/{body["file"]}')
        variants = case_statistics.get_variants_df(log_df)
        return jsonify(variants.values.tolist())
    else:
        return {
            "error": "Please provide a filename"
        }, 404


# A route to return all case variants in a log.
@app.route('/start_end_activities', methods=['POST'])
def get_start_end_activities() -> dict:
    """ Returns a list of start and end activities.

    Returns:
        dict: A dict with start and end activities.
    """
    body = request.json

    if 'file' in body:
        log_df = read_log(f'{data_path}/{body["file"]}')
        return calc_start_end_activities(log_df)
    else:
        return {
            "error": "Please provide a filename"
        }, 404


# A route to get all columns in a log.
@app.route('/get_columns', methods=['POST'])
def get_columns() -> list:
    """ Returns a list of column names.

    Returns:
        list: A list with all the column names.
    """
    body = request.json

    if 'file' in body:
        log = read_log(f'{data_path}/{body["file"]}')
        filtered_columns = filter(lambda column: column != 'time:timestamp', log.columns)
        return jsonify(list(filtered_columns))
    else:
        print("File not found")
        return jsonify([]), 404


# A route to return all values of a column in a log.
@app.route('/get_column_values', methods=['POST'])
def get_column_values() -> list:
    """ Returns a list of column names.

    Returns:
        list: A list with all the column names.
    """
    body = request.json

    if 'file' in body:
        if 'column' in body:
            log = read_log(f'{data_path}/{body["file"]}')
            values = attributes_filter.get_attribute_values(log, attribute_key=body.get('column'))
            return jsonify(list(values))
        else:
            print("Please provide a column name")
            return jsonify([]), 404
    else:
        print("Please provide a filename")
        return jsonify([]), 404


if __name__ == '__main__':
    app.run(debug=True)
