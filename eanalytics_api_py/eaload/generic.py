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
            sep=';',
            quotechar='"',
            compression='gzip',
            encoding='utf-8',
            index_col=None,
            header=0,
            **kwargs,
        )
        l_df.append(df)

    df_concat = _pd.concat(l_df, axis=0, ignore_index=True)
    __set_df_col_dtypes(df_concat)
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
        
    # datetime
    if "order_date_epoch" in df.columns:
        df["order_date_epoch"] = _pd.to_datetime(df["order_date_epoch"],unit='s')

    if "channel_lvl_date" in df.columns:
        df["channel_lvl_date"] = _pd.to_datetime(df["channel_lvl_date"],unit='s')
    
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
            
    # uint-8
    for col_name in [
        "a_channel_sz",
        "a_orderproduct_sz",
        "channel_lvl_position",
        "product_position",
        "orderproduct_quantity",
    ]:
        if col_name in df.columns:
            df[col_name] = df[col_name].astype('uint8')
    
    for col_name in df.columns:
        if col_name.startswith('channel_lvl_p'):
            df[col_name] = df[col_name].astype('category')
        elif df[col_name].dtype == 'object':
            df[col_name] = df[col_name].astype('category')
        elif df[col_name].dtype == 'int64':
            df[col_name] = df[col_name].astype('uint32')
        elif df[col_name].dtype == 'float64':
            df[col_name] = df[col_name].astype('float32')
            
    return df