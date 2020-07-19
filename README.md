# eanalytics_api_py

A python 3 module to retrieve data from Eulerian Technologies API

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

## Prerequisites

Having a python 3 environment

## Installing

```
pip3 install eanalytics_api_py
```

## Upgrading

```
pip3 install eanalytics_api_py --upgrade
```

## Running

### Connexion class

```
from  eanalytics_api_py.conn import Conn
from  eanalytics_api_py.datamining_helper import deduplicate_product_cols_file_2_df
import pandas as pd

language = 'en'

conn = Conn(
        gridpool_name='demo',
        datacenter='com',
        api_key='key',
        print_log = True
)
```
### download_datamining method

```
path2file = conn.download_datamining(
                website_name = 'demo',
                datamining_type = 'order',
                payload = {
                    'date-from':'05/01/2020',
                    'date-to':'05/01/2020',
                    'with-ordertype':1,
                    'with-mdevicetype' : 1,
                    'with-last-channel':1,
                    'with-cgiparam':1,
                    'with-channel-count':1,
                    'with-channel-level':1
                    'with-orderproduct' : 1,
                    'with-productparam' : 1,
                    'with-productgroup' : 1,
                    'ea-lg' : language,
                },
                output_filename = 'demo_orders.csv.gzip',
                output_directory = '',
                override_file=True
)

# load into pandas dataframe and normalize product columns
df = deduplicate_product_cols_file_2_df(
        path2file,
        language=language
)
```
- Use [this doc](https://doc.api.eulerian.com/#tag/Datamining:-sales%2Fpaths%2F~1ea~1%7Bsite%7D~1report~1order~1download.json%2Fget) to customize your payload object: 

### download_edw method
```
query = """ GET {
  TIMERANGE { 1571912419 1571936095 }
  READERS {
    ea:order@demo AS order
  }
  OUTPUTS_ROW( order ) {
    order.uid, order.timestamp, order.orderref, order.orderstatus, order.amount
  }
};"""


path2file = conn.download_edw(
            query=query,
            ip='ip1,ip2,ip3',
            token_path2file = 'edw_token.json',
            force_token_refresh = False,
            output_filename = 'edw_test.csv.gzip',
            output_directory='',
           # jobrun_id = 0,
            override_file=True,
)
```
[Eulerian Datawharehouse documentation](https://doc.api.eulerian.com/#section/EDW)
TODO: query builder doc

## Notes
- The file downloaded is gzipped
- Explore your data using jupyter-notebook along with pandas and seaborn for plug'n'play data cleaning/visualisation
- Samples jupyter notebooks featuring different use-cases will be uploaded in the near future
- A docker image containing jupyter and our librairy is available [here](https://hub.docker.com/repository/docker/soloan/eanalytics-jupyter/general)

## Author

* **Florian Dauphin** - https://github.com/Afilnor
