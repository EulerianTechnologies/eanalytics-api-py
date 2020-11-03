"""This module allows to manipulate a pandas DataFrame"""

import pandas as _pd

def move_columns(
    df : _pd.DataFrame,
    colname_idx : dict,
    inplace : bool = False,
) -> _pd.DataFrame:
    """ For a given DataFrame, move given column_name keys into the given index values

    Parameters
    ----------
    df : pd.DataFrame, mandatory

    colname_idx: dict, mandatory
        { "colname1" : 2, "colname10" : 1 }

    Returns
    -------
    pf.DataFrame with updated columns
    """

    if not inplace:
        df=df.copy()

    if not isinstance(df, _pd.DataFrame):
        raise TypeError("df should be a pandas DataFrame")

    if not isinstance(colname_idx, dict):
        raise TypeError("colname_idx should be a dict")

    for column_name in colname_idx.keys():
        if not isinstance(column_name, str):
            raise TypeError(f"key={column_name}, should be an str type")

        if column_name not in df.columns:
            raise ValueError(f"key={column_name} is not a column of the given DataFrame")

        idx = colname_idx[column_name]

        if not isinstance(idx, int):
            raise TypeError(f"key={column_name}, val={idx} should be an integer type")

        if df.columns.get_loc(column_name) == idx:
            next

        tmp_col = df[column_name]

        df.drop(
            labels=[column_name],
            axis=1,
            inplace=True
        )

        df.insert(
            loc=idx,
            column=column_name,
            value=tmp_col
        )

        return df

def query_string_to_column(
    df : _pd.DataFrame,
    col_name : list,
    n_decode : int =  0,
) -> _pd.DataFrame:
    """ For a given DataFrame, move given column_name keys into the given index values

    Parameters
    ----------
    df : pd.DataFrame, mandatory

    col_name: list, mandatory
       The columns in which to retrieve the query strings

    Returns
    -------
    pf.DataFrame with updated columns
    """