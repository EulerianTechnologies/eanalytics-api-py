import pandas as _pd
import gzip as _gzip
import re as _re
from .generic import __set_df_col_dtypes

def deduplicate_touchpoints_cols_file_2_df(
    path2files : list,
    sep=';',
    quotechar='"',
    compression='gzip',
    encoding='utf-8',
    **kwargs
):
    """ Deduplicate product params columns

        In eulerian datamining, each product related column is duplicated by the number of products.
        The goal is to perform the following transformation (example):
            product-color # 1,  product-color # 2, product-color # 3, product-color # 4, product-color # 5,
        Into
            product-color
        
        This duplicates row for every product and normalize the product param columns.
        Rows having an empty product ref will be discarded.

        Parameters
        ----------
        path2files : list, obligatory
            The targeted list of path2files

        prdref_colname : str, optional

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

    if isinstance(path2files, str):
        path2files = [ path2files ]

    if not isinstance(path2files, list):
        raise TypeError("path2files should be either a str or a list")
    
    channel_colname = "channel_lvl_p0"
    channel_lvl_regex = r'channel_lvl(\d+)_[\w\W]+'

    l_df = []
    for path2file in path2files:
        l_sub_df = []
        n_touch = set()
        non_touch_cols = []
        touch_cols = set()

        # get the headers
        with _gzip.open(path2file, "rt") as f:
         for line in f:        
            head = line
            break
        
        for col_name in head.replace('"', "").split(sep):
            col_name = col_name.strip()

            if col_name.startswith("channel_lvl"):
                match = _re.search(
                    pattern = channel_lvl_regex,
                    string = col_name
                )
                lvl = match.group(1)
                n_touch.add(int(lvl))
                touch_cols.add(col_name.replace( # leftmost replace
                    lvl,
                    '{}',
                    1
                ))

            else:
                non_touch_cols.append(col_name)

        if len(n_touch) == 0:
            raise SystemError(f"No products found\
                \n Did you add the following parameters in the Conn.download_datamining payload ?\
                \n 'with-channel-level' : 1,\
                \n 'with-channel-count' : 1,\
                \n 'max-channel-level' : 40,\
                \n max-channel-info' : 9,\
             ")

        for i in range(max(n_touch)+1):
            col_names = non_touch_cols + list( map( lambda x: x.format(i), touch_cols ) )

            df = _pd.read_csv(
                path2file,
                compression=compression,
                sep=sep,
                quotechar=quotechar,
                encoding=encoding,
                usecols=col_names,
                index_col=None,
                **kwargs,
            )

            columns = []
            for col_name in df.columns:
                if col_name.startswith("channel_lvl"):
                    match = _re.search(
                        pattern = channel_lvl_regex,
                        string = col_name
                    )
                    lvl = match.group(1)
                    col_name = col_name.replace( # leftmost replace
                        lvl,
                        '',
                        1
                    )
                columns.append(col_name)
            df.columns = columns

            if channel_colname not in df.columns:
                raise KeyError(f"channel column={channel_colname} not found")

            # add position
            df['channel_lvl_position'] = i

            # discard rows with empty product ref
            df = df[df[channel_colname] != "-"]

            l_sub_df.append(df)
                                   
        sub_df = _pd.concat(l_sub_df, axis=0, ignore_index=True)
        l_df.append(sub_df)

    df_concat = _pd.concat(l_df, axis=0, ignore_index=True)
    __set_df_col_dtypes(df_concat)
    return df_concat

def deduplicate_product_cols_file_2_df(
    path2files : list,
    sep=';',
    quotechar='"',
    compression='gzip',
    encoding='utf-8',
    **kwargs
):
    """ Deduplicate product params columns

        In eulerian datamining, each product related column is duplicated by the number of products.
        The goal is to perform the following transformation (example):
            product-color # 1,  product-color # 2, product-color # 3, product-color # 4, product-color # 5,
        Into
            product-color
        
        This duplicates row for every product and normalize the product param columns.
        Rows having an empty product ref will be discarded.

        Parameters
        ----------
        path2files : list, obligatory
            The targeted list of path2files

        prdref_colname : str, optional

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

    if isinstance(path2files, str):
        path2files = [ path2files ]

    if not isinstance(path2files, list):
        raise TypeError("path2files should be either a str or a list")
    
    prdref_colname = "orderproduct_ref"
    prd_colnames = [
        "orderproduct_name",
        "orderproduct_quantity",
        "orderproduct_ref",
        "orderproduct_sellprice",
        "productgroup_margin",
        "productgroup_name",
        "productparam" 
    ]

    l_df = []
    for path2file in path2files:
        l_sub_df = []
        n_products = set()
        non_product_cols = []
        product_cols = set()

        # get the headers
        with _gzip.open(path2file, "rt") as f:
         for line in f:        
            head = line
            break
        
        for col_name in head.replace('"', "").split(sep):
            col_name = col_name.strip()

            if col_name.startswith(tuple(prd_colnames)):
                n_products.add(int(col_name[-1])) # the idx
                product_cols.add(col_name[:-1]+'{}') # the rest

            else:
                non_product_cols.append(col_name)

        if len(n_products) == 0:
            raise SystemError(f"No products found\
                \n Did you add the following parameters in the Conn.download_datamining payload ?\
                \n 'with-orderproduct' : 1,\
                \n 'with-productparam' : 1,\
                \n 'with-productgroup' : 1,\
             ")

        for i in range(max(n_products)+1):
            col_names = non_product_cols + list( map( lambda x: x.format(i), product_cols ) )

            df = _pd.read_csv(
                path2file,
                compression=compression,
                sep=sep,
                quotechar=quotechar,
                encoding=encoding,
                usecols=col_names,
                index_col=None,
                **kwargs,
            )

            columns = []
            for col_name in df.columns:
                if col_name.startswith(tuple(prd_colnames)):
                    col_name = col_name[:-2]
                columns.append(col_name)
            df.columns = columns

            if prdref_colname not in df.columns:
                raise KeyError(f"product ref  column={prdref_colname} not found")

            # add position
            df['product_position'] = i

            # discard rows with empty product ref
            df = df[df[prdref_colname] != 0]
            df = df[df[prdref_colname] != "0"]

            l_sub_df.append(df)
                                   
        sub_df = _pd.concat(l_sub_df, axis=0, ignore_index=True)
        l_df.append(sub_df)

    df_concat = _pd.concat(l_df, axis=0, ignore_index=True)
    __set_df_col_dtypes(df_concat)
    return df_concat