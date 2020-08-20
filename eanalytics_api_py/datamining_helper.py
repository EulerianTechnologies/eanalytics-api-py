import pandas as pd
import gzip
import re

def deduplicate_product_cols_file_2_df(
    path2files : list,
    prdref_colname="",
    sep=';',
    quotechar='"',
    compression='gzip',
    encoding='utf-8',
    language='en',
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

        language : str, optional
            The language of the downloaded datamining file
            Default: 'en'  (fr/es/en)
            Note: Used to detect the product reference column

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

    regex_prdp = r"(\s#\s)(\d+)$"
    prdref_colname_map = {
        "fr" : "référence du produit",
        "en" : "product SKU",
        "es" : "referencia del producto"
    }

    if language not in prdref_colname_map:
        raise KeyError(f"accepted language={', '.join(prdref_colname_map.keys())}")

    if not prdref_colname:
        prdref_colname = prdref_colname_map[language]
    
    l_df = []
    for path2file in path2files:
        l_sub_df = []
        n_products = set()
        non_product_cols = []
        product_cols = set()

        # get the headers
        with gzip.open(path2file, "rt") as f:
         for line in f:        
            head = line
            break
        
        for col_name in head.replace('"', "").split(";"):
            col_name = col_name.strip()
            group =  re.search(regex_prdp, col_name)

            if group:
                n_products.add(int(group.group(2)))
                product_cols.add(
                    re.sub(
                        pattern=regex_prdp,
                        repl=r"\1{}",
                        string=col_name
                    )
                )

            else:
                non_product_cols.append(col_name)

        if len(n_products) == 0:
            raise SystemError(f"No products found\
                \n Did you add the following parameters in the Conn.download_datamining payload ?\
                \n 'with-orderproduct' : 1,\
                \n 'with-productparam' : 1,\
                \n 'with-productgroup' : 1,\
             ")

        for i in range(1,max(n_products)+1):
            col_names = non_product_cols + list( map( lambda x: x.format(i), product_cols ) )

            df = pd.read_csv(
                path2file,
                compression=compression,
                sep=sep,
                quotechar=quotechar,
                encoding=encoding,
                usecols=col_names,
                index_col=None,
                **kwargs,
            )

            df.columns = [ re.sub(pattern=regex_prdp, repl="", string=col_name) for col_name in df.columns ]

            if prdref_colname not in df.columns:
                raise KeyError(f"product ref  column={prdref_colname} not found")

            # discard rows with empty product ref
            df = df[df[prdref_colname] != 0]
            df = df[df[prdref_colname] != "0"]

            l_sub_df.append(df)
                                   
        sub_df = pd.concat(l_sub_df, axis=0, ignore_index=True)
        l_df.append(sub_df)

    return pd.concat(l_df, axis=0, ignore_index=True)

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
        df = pd.read_csv(
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

    return pd.concat(l_df, axis=0, ignore_index=True)