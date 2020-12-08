""" Generic load from csv file into pandas DataFrame with transformation """

import re as _re
import pandas as _pd

def csv_files_2_df(
    path2files : list,
    sep=';',
    quotechar='"',
    compression='gzip',
    encoding='utf-8',
    **kwargs
):
    """ Load a list of csv files into a pandas dataframe

    Parameters
    ----------
    path2files : list, obligatory
        The targeted list of path2files

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
    l_df = []

    if isinstance(path2files, str):
        path2files = [ path2files ]

    if not isinstance(path2files, list):
        raise TypeError("path2files should be either a str or a list type")

    for path2file in path2files:
        df = _pd.read_csv(
            path2file,
            sep=sep,
            quotechar=quotechar,
            compression=compression,
            encoding=encoding,
            index_col=None,
            header=0,
            **kwargs,
        )
        l_df.append(df)

    df_concat = _pd.concat(l_df, axis=0, ignore_index=True)
    __set_df_col_dtypes(df_concat)

    # if view-id=0 similuate attrib view col
    if "viewchannel_lvl_p0" not in df_concat.columns:
        for col_name in df_concat:
            if col_name.startswith("channel_lvl0_"):
                newcol_name = _re.sub(
                    pattern=r"^channel_lvl0_(.+)$",
                    repl=r"viewchannel_\g<1>",
                    string=col_name
                )
                df_concat[newcol_name] = df_concat[col_name]

    # rename to match new convention
    else:
        d_col_rename_map = {}
        for col_name in df_concat:
            new_colname = _re.sub(
                pattern=r"^(viewchannel)_lvl_(.+)$",
                repl=r"\g<1>_\g<2>",
                string=col_name
            )
            d_col_rename_map[col_name] = new_colname
        df_concat = df_concat.rename(
            columns=d_col_rename_map
        )

    return df_concat

def __set_df_col_dtypes( df : _pd.DataFrame() ):
    """ Load a list of csv files into a pandas dataframe

    Parameters
    ----------
    df : pd.DataFrame, obligatory

    Returns
    -------
    pd.Dataframe
        Pandas dataframe object with dtype updated
    """
    if not isinstance(df, _pd.DataFrame):
        raise TypeError("argument should be a pd.DataFrame")

    # category
    for col_name in [
        "order_status",
        "channel_lvl_via",
        "channel_lvl_profile",
        "ordertype_key",
        "orderproduct_ref",
        "orderproduct_name",
        "productgroup_name",
    ]:
        if col_name in df.columns:
            df[col_name] = df[col_name].astype('category')

    # int-16
    for col_name in [
        "a_channel_sz",
        "a_orderproduct_sz",
        "channel_lvl_position",
        "product_position",
        "orderproduct_quantity",
    ]:
        if col_name in df.columns:
            df[col_name] = df[col_name].astype('int16')

    for col_name in df.columns:
        if col_name.startswith('channel_lvl_p'):
            df[col_name] = df[col_name].astype('category')
        elif df[col_name].dtype == 'object':
            df[col_name] = df[col_name].astype('category')

    return df
