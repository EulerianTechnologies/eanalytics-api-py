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

See [sample notebooks](notebooks)

- [Eulerian datamining api doc](https://doc.api.eulerian.com/#tag/Datamining:-sales%2Fpaths%2F~1ea~1%7Bsite%7D~1report~1order~1download.json%2Fget) to customize your payload object: 
- [Eulerian Datawarehouse api doc](https://doc.api.eulerian.com/#section/EDW)

## Notes
- The file downloaded is gzipped
- Explore your data using jupyter-notebook along with pandas and seaborn for plug'n'play data cleaning/visualisation
- Samples jupyter notebooks featuring different use-cases will be uploaded in the near future
- A docker image containing jupyter and our librairy is available [here](https://hub.docker.com/repository/docker/soloan/eanalytics-jupyter/general)

## Author

* **Florian Dauphin** - https://github.com/Afilnor
