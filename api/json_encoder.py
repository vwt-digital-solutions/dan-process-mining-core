from flask.json import JSONEncoder
import numpy as np


class CustomJSONEncoder(JSONEncoder):
    """ A custom JSON encoder which turns numpy types into default types. """

    def default(self, obj: dict) -> dict:
        """ Changes numpy types to python standard types.

        Args:
            obj (dict):
                JSON object

        Returns:
            dict:
                A cleaned JSON object.
        """
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):
            return int(obj)

        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)

        elif isinstance(obj, (np.complex_, np.complex64, np.complex128)):
            return {'real': obj.real, 'imag': obj.imag}

        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()

        elif isinstance(obj, (np.bool_)):
            return bool(obj)

        elif isinstance(obj, (np.void)):
            return None

        return JSONEncoder.default(self, obj)
