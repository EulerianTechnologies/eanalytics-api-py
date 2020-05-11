# Eulerian-Analytics

A python 3 module to download locally a datamining dataset from the Eulerian Technologies API

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

## Prerequisites

Having a python 3 environment

## Installing

```
pip install eulerian_analytics_py_api
```

## Running

```
from eulerian_analytics_py_api.datamining import download
path2file = download(
                gridpool_name = grid,
                datacenter = datacenter,
                website_name = website,
                api_key = key,
                datamining_type = type
                payload = {},
)
```

## Notes

- The file downloaded is gzipped
- Explore your data using jupyter-notebook along with pandas and seaborn for plug'n'play data cleaning/visualisation
- Samples jupyter notebooks featuring different use-cases will be uploaded in the near future

## Author

* **Florian Dauphin** - https://github.com/Afilnor
