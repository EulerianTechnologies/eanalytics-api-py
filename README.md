# eanalytics_api_py

A python 3 module to retrieve data from Eulerian Technologies API

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

## Prerequisites

Having a python 3 environment

## Installing

```
pip install eanalytics_api_py
```

## Running

```
from eanalytics_api_py.datamining import download
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

- Use this doc to customize your payload object: https://doc.api.eulerian.com/#tag/Datamining:-goals%2Fpaths%2F~1ea~1%7Bsite%7D~1report~1actionlog~1search.json%2Fget
- The file downloaded is gzipped
- Explore your data using jupyter-notebook along with pandas and seaborn for plug'n'play data cleaning/visualisation
- Samples jupyter notebooks featuring different use-cases will be uploaded in the near future

## Author

* **Florian Dauphin** - https://github.com/Afilnor
