""" Load datamining csv file into pandas DataFrame
Specific transformation available for easier analysis
"""

import re as _re

import pandas as _pd
import numpy as np
from .generic import csv_files_2_df


def deduplicate_touchpoints(
        source: list,
        sep=';',
        quotechar='"',
        compression='gzip',
        encoding='utf-8',
        **kwargs
):
    """ Deduplicate marketing touchpoints

        Parameters
        ----------
        source : list of path2file OR pandas DataFrame

        sep : str, obligatory
            The csv sep char
            Default: ';'

        quotechar : str, optional
            The csv quote char
            Default: '"'

        compression : str, optional
            The file compression algorithm
            Default: 'gzip'

        encoding: str, optional
            The file encoding
            Default: 'utf-8'

        **kwargs:
            Keyword arguments for pd.read_csv function


    Returns
    -------
    pd.Dataframe
        Pandas DataFrame object
    """

    if isinstance(source, str):
        source = [source]

    if isinstance(source, list):
        df = csv_files_2_df(
            path2files=source,
            sep=sep,
            quotechar=quotechar,
            compression=compression,
            encoding=encoding,
            **kwargs,
        )

    elif isinstance(source, _pd.DataFrame):
        df = source

    else:
        raise ValueError("source should be a path2file or a pandas DataFrame")

    columns = []
    stubnames = []

    for col_name in df.columns:
        if col_name.startswith('channel_lvl'):
            col_name = _re.sub(
                pattern=r'^(channel)_lvl(\d+)_(.+)$',
                repl=r"\g<1>_\g<3>_\g<2>",
                string=col_name
            )
            prefix = _re.sub(
                pattern=r'^(channel_.+_)(\d+)$',
                repl=r"\g<1>",
                string=col_name
            )
            if prefix not in stubnames:
                stubnames.append(prefix)
        columns.append(col_name)

    df.columns = columns

    df = _pd.wide_to_long(
        df,
        stubnames=stubnames,
        i="order_ref",
        j="channel_position",
    ).reset_index()

    columns = []
    for col_name in df.columns:
        if col_name.startswith('channel_') and col_name.endswith('_'):
            col_name = col_name[:-1]
        columns.append(col_name)

    df.columns = columns
    df = df[~df.channel_p0.isin(['-', 0, None, '0', np.nan])]

    return df


def deduplicate_products(
        source: list,
        sep=';',
        quotechar='"',
        compression='gzip',
        encoding='utf-8',
        **kwargs
):
    """ Deduplicate product params columns

    In eulerian datamining, each product related column is duplicated
    by the number of products.
    The goal is to perform the following transformation (example):
        product-color # 1,  product-color # 2, product-color # 3
    Into
        product-color

    This duplicates row for every product and normalize the product param columns.
    Rows having an empty product ref will be discarded.

    Parameters
    ----------
    source : list, obligatory
        The targeted list of path2files or pandas DataFrame

    sep : str, obligatory
        The csv sep char
        Default: ';'

    quotechar : str, optional
        The csv quote char
        Default: '"'

    compression : str, optional
        The file compression algorithm
        Default: 'gzip'

    encoding: str, optional
        The file encoding
        Default: 'utf-8'

    **kwargs:
        Keyword arguments for pd.read_csv function


    Returns
    -------
    pd.Dataframe
        Pandas dataframe object
    """

    if isinstance(source, str):
        source = [source]

    if isinstance(source, list):
        df = csv_files_2_df(
            path2files=source,
            sep=sep,
            quotechar=quotechar,
            compression=compression,
            encoding=encoding,
            **kwargs,
        )

    elif isinstance(source, _pd.DataFrame):
        df = source

    else:
        raise ValueError("source should be a path2file or a pandas DataFrame")

    stubnames = []
    columns = []

    prd_colnames = [
        "orderproduct_name",
        "orderproduct_quantity",
        "orderproduct_ref",
        "orderproduct_sellprice",
        "productgroup_margin",
        "productgroup_name",
        "productparam_"
    ]

    # orderproduct_name_i
    for col_name in df.columns:
        for prd_col in prd_colnames:
            if col_name.startswith(prd_col):
                l_col_name = col_name.split("_")
                stubname = "_".join(l_col_name[:-1])  # orderproduct_name
                idx = l_col_name[-1]  # i
                col_name = stubname + idx  # orderproduct_namei
                if stubname not in stubnames:
                    stubnames.append(stubname)
        columns.append(col_name)

    if len(stubnames) == 0:
        raise SystemError(f"No product column found starting by: {', '.join(prd_colnames)}")

    df.columns = columns

    for stubname in stubnames:
        if stubname in columns:
            raise ValueError(f"stubname={stubname} should not equal to a column name")

    df = _pd.wide_to_long(
        df=df,
        stubnames=stubnames,
        i="order_ref",
        j="product_position"
    ).reset_index()
    df = df[~df.orderproduct_ref.isin(['-', 0, None, '0', np.nan])]

    return df
