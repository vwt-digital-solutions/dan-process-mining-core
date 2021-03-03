from textwrap import dedent
import json

suggestedColumns = {
    "time:timestamp:start": {
        "column": "starttime",
        "message": """
            If you don't add a starttime column the duration of an activity can not be calculated.
            Only the time between activities will be calculated
        """
    }
}

requiredColumns = {
    "time:timestamp": "endtime",
    "case:concept:name": "project number",
    "concept:name": "activity"
}


class SuggestedColumnWarning(UserWarning):
    @staticmethod
    def warn(column: str, description: dict):
        message = f"""
            Missing optional column: "{column}" ({description["column"]})
            {description["message"]}
        """

        return dedent(message)


class RequiredColumnError(Exception):
    def __init__(self, path: str, colmap: dict, column: str):
        self.column = column
        self.colmap = colmap
        self.path = path

    def __str__(self):
        new_colmap = {
            **self.colmap,
            f"your_{requiredColumns[self.column].replace(' ', '_')}_column": self.column
        }
        message = f"""
            Missing "{self.column}" column -> please rename your {requiredColumns[self.column]} column to "{self.column}"
            Alternatively replace your read_csv with the following:
            ```
            read_csv({self.path}, {
                json.dumps(new_colmap)
            })
            ```
        """

        return dedent(message)
