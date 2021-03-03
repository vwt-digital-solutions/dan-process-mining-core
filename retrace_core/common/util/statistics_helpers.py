from typing import Union

import pandas as pd

DataFrame_or_Series = Union[pd.Series, pd.DataFrame]


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

        >>> normalize(pd.DataFrame(data={"col1": [1,2,3,4], "col2": [0,50,75,100]}))
               col1  col2
        0  0.000000  0.00
        1  0.333333  0.50
        2  0.666667  0.75
        3  1.000000  1.00

    """
    return (table - table.min()) / (table.max() - table.min())
