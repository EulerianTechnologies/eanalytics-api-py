### 0.1.45
- Final compression of CSV file is now optional( compress=True | False )
- Add an extra argument to download_edw() function used to enable transport layer compression ( encoding=gzip|identity )

### 0.1.45
- Avoid error when killing an Analysis who was already recycled.

### 0.1.44
- Remove some traces.

### 0.1.43
- Avoid being connected during JSON -> CSV -> GZ convertions

### 0.1.42
- fix EDW new hostname

### 0.1.41
- download_* methods: now check if requested website_name is allowed for current user
- download_edw: dump urllib response if ijson.common.IncompleteJSONError occurs
- download_edw: properly kill job uuid

### 0.1.40
- Explicit call to action for BlockingIOError in Conn.download_edw
- Solved issue where "p1" column appeared twice in Conn.download_flat_overview_realtime_report 

### 0.1.39
- fix path for SL channel in Conn.download_float_overview_realtime_report
- set correct pandas column type for realtime report and flat overview

### 0.1.38
- notebook edw_raw_clickview
- typo in filename in Conn.download_edw

### 0.1.37
- conn.download_flat_realtime_report, incorrectly setting dtype=int64
for floats

### 0.1.36
- conn.download_realtime_report: fix when date in absent in the response json

### 0.1.35
- conn.download_datamining: deep copy the object payload to avoid
modification on the original object

### 0.1.34
- New methods for Conn class to retrieve orderpayment and estimatetype

### 0.1.33
- fix prev version typo

### 0.1.32
- Support for more channels in [flat_download_overview_realtime_report](eanalytics_api_py/conn/_download_flat_overview_realtime_report.py)

### 0.1.31
- new Conn method [flat_download_overview_realtime_report](eanalytics_api_py/conn/_download_flat_overview_realtime_report.py)
- [see notebook](notebooks/flat_overview_realtime_report.ipynb)

### 0.1.30
- new Conn method [flat_download_realtime_report](eanalytics_api_py/conn/_download_flat_realtime_report.py)
- [see notebook](notebooks/flat_realtime_report.ipynb)

### 0.1.2
- created module for Conn class
- Conn class methods into separate files

### 0.1.1
- fixed bug in datamining.deduplicate_products where stubnames were not generated correctly

### 0.1.0
- more unit testing
- cleaned up the Conn class

### 0.0.99
bug fix, casting payload['view-id'] to str before regex check

### 0.0.98
Implemented a proper logger for Conn class

### 0.0.97
pylint review + tests

### 0.0.96
tests + internal doc/opti

### 0.0.95
- [extra documentation for datamining payload](eanalytics_api_py/earequest/datamining/payload.py)
- [new transformation module to work on pandas DataFrame](eanalytics_api_py/eatransform)
- [new functions in eajupyter](eanalytics_api_py/eajupyter/html.py)
-  - button_hide_input_cells
-  - remove_class_output_scroll

### 0.0.94
internal optimizations

### 0.0.93
fix packaging + update payload

### 0.0.92
[wrapper for datamining payload params](eanalytics_api_py/earequest/datamining/payload.py)

### 0.0.91
[conn.get_website_by_name](eanalytics_api_py/conn/__init__.py)

### 0.0.90
[filtered out rows without products/marketing touches](eanalytics_api_py/eaload/datamining.py)

### 0.0.89
deployment workflow mattermost hook secret fix
notebook typo

### 0.0.88
deployment workflow fix

### 0.0.87
[fix](eanalytics_api_py/eaload/datamining.py)
strip correctly col_name to push into stubnames array

### 0.0.86
fix in [eaload/generic.py](eanalytics_api_py/eaload/generic.py)
streamline view column name

### 0.0.85
typo

### 0.0.84
fix in [eaload/generic.py](eanalytics_api_py/eaload/generic.py)
with view-id=0, add viewchannel_X to match other views


### 0.0.83
fix in [eaload.datamining](eanalytics_api_py/eaload/datamining.py)
deduplicate_touchpoints and deduplicate_products can be used with either
csv or pandas Dataframe

### 0.0.82
uint dtype to int

### 0.0.81
fix[cgiparam download datamining](eanalytics_api_py/conn/__init__.py)

### 0.0.80
fix in [eaload.datamining.deduplicate_product_cols_file_2_df](eanalytics_api_py/eaload/datamining.py)

### 0.0.79
fix edw auth token in [download_edw](eanalytics_api_py/conn/__init__.py)

### 0.0.78
changes in [download_edw](eanalytics_api_py/conn/__init__.py)
do not store the edw access token locally anymore

### 0.0.77
[new download_realtime_report](eanalytics_api_py/conn/__init__.py)
it is a simple API which will evolve in the feature (waiting for backend)
[notebook](notebooks/realtime_report.ipynb)

### 0.0.76
[different functions to load csv files into dataframe](eanalytics_api_py/eaload)
[different functions to create widgets and buttons to interact with jupyter ](eanalytics_api_py/eajupyter)
[get started notebooks](notebooks)
now use internal eulerian name as header (more consistent)
pandas column dtype automatically set in eaload functions

### 0.0.75
fix Conn.download_datamining method day downloaded multiple times
various fix

### 0.0.74
improve [jupyter_helper.hide_cells()](eanalytics_api_py/eajupyter/jupyter_helper.py)
various fix

### 0.0.73
fix datamining helper

### 0.0.72
Conn.download_datamining
- return now an array of files instead of a string
- deletion of params jobrun_id / output_filename
- new param n_slice_days (default=31) split query to datamining by slice
[new csv_files_2_df method](eanalytics_api_py/jupyter/jupyter_helper.py)

### 0.0.71
log http get request in Conn.download_datamining method

### 0.0.70
Packaging fix

### 0.0.69
[New helper method](eanalytics_api_py/jupyter/jupyter_helper.py)
Jupyter: Add a html button to trigger cell visibility according ot the given tag code
```
from  eanalytics_api_py import jupyter_helper
jupyter_helper.hide_cells(tag=["code"])
```

### 0.0.68
[get_view_id_name_map](eanalytics_api_py/conn/__init__.py) Added default view_id=0

### 0.0.67
New Conn method [get_view_id_name_map](eanalytics_api_py/conn/__init__.py)

### 0.0.66
new param status_waiting_seconds for download_edw and download_datamining method

### 0.0.65
Optimized download datamining with a lot of data  [datamining_helper](eanalytics_api_py/conn/__init__.py)

### 0.0.64
Fix indexing issue in [datamining_helper](eanalytics_api_py/datamining_helper.py)

### 0.0.63
New module: datamining_helper.py
New method: deduplicate_product_cols_file_2_df
See [here](eanalytics_api_py/datamining_helper.py)
