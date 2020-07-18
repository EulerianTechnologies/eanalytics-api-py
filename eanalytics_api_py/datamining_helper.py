import pandas as pd
import gzip
import re

def deduplicate_product_cols_file_2_df(
    path2file : str,
    sep=';',
    quotechar='"',
    compression='gzip',
    encoding='utf-8',
    language='en'
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
        path2file : str, obligatory
            The targeted path2file

        sep : str, obligatory
            The csv sep char
            Default: ';'

        quotechar : str, obligatory
            The csv quote char
            Default: '"'

        compression : str, obligatory
            The file compression algorithm
            Default: 'gzip'

        encoding: str, optional
            The file encoding
            Default: 'utf-8'

        language : str, obligatory
            The language of the downloaded datamining file
            Default: 'en'  (fr/es/en)
            Note: Used to detect the product reference column

    Returns
    -------
    pd.Dataframe
        Pandas dataframe object
    """
    regex_prdp = r'(\s#\s)(\d+)$'
    df_list = []
    n_products = set()
    non_product_cols = []
    product_cols = set()
    prdref_colname_map = {
        'fr' : 'référence du produit',
        'en' : 'product SKU',
        'es' : 'referencia del producto'
    }
    prdref_colname = 'en' if language not in prdref_colname_map else prdref_colname_map[language]
    
    # get the headers
    with gzip.open(path2file, 'rt') as f:
     for line in f:        
        head = line
        break
    
    for col_name in head.replace('"', '').split(';'):
        col_name = col_name.strip()
        group =  re.search(regex_prdp, col_name)
        if group:
            n_products.add(int(group.group(2)))
            product_cols.add(re.sub(pattern=regex_prdp, repl=r'\1{}', string=col_name))
        else:
            non_product_cols.append(col_name)

    if len(n_products) == 0:
        raise SystemError(
        """No products found, did you add the following parameters in the Conn.download_datamining payload ?
         'with-orderproduct' : 1,
         'with-productparam' : 1,
         'with-productgroup' : 1,
         """
         )

    for i in range(1,max(n_products)+1):
        col_names = non_product_cols + list( map( lambda x: x.format(i), product_cols ) )

        df = pd.read_csv(
            path2file,
            compression=compression,
            sep=sep,
            quotechar=quotechar,
            encoding=encoding,
            usecols = col_names,
        )

        df.columns = [ re.sub(pattern=regex_prdp, repl='', string=col_name) for col_name in df.columns ]

        if prdref_colname not in df.columns:
            raise KeyError(f"product ref  column={prdref_colname} not found")

        # discard rows with empty product ref
        df = df[df[prdref_colname] != 0]
        df = df[df[prdref_colname] != '0']

        df_list.append(df)
                                           
    return pd.concat(df_list)