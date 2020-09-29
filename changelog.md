### 0.0.81
fix[cgiparam download datamining](eanalytics_api_py/conn.py)

### 0.0.80
fix in [eaload.datamining.deduplicate_product_cols_file_2_df](eanalytics_api_py/eaload/datamining.py)

### 0.0.79
fix edw auth token in [download_edw](eanalytics_api_py/conn.py)

### 0.0.78
changes in [download_edw](eanalytics_api_py/conn.py)
do not store the edw access token locally anymore

### 0.0.77
[new download_realtime_report](eanalytics_api_py/conn.py)
it is a simple API which will evolve in the feature (waiting for backend)
[notebook](notebooks/realtime_report.ipynb)

### 0.0.76
[different functions to load csv files into dataframe](eanalytics_api_py/eaload)
[different functions to create widgets and buttons to interect with jupyter ](eanalytics_api_py/eajupyter)
[get started notebooks](notebooks/)
now use internal eulerian name as header (more consistent)
pandas column dtype automatically set in eaload functions

### 0.0.75
fix Conn.download_datamining method day downloaded multiple times
various fix

### 0.0.74
improve [jupyter_helper.hide_cells()](eanalytics_api_py/jupyter/jupyter_helper.py)
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
[get_view_id_name_map](eanalytics_api_py/conn.py) Added default view_id=0

### 0.0.67
New Conn method [get_view_id_name_map](eanalytics_api_py/conn.py)

### 0.0.66
new param status_waiting_seconds for download_edw and download_datamining method

### 0.0.65
Optimized download datamining with a lot of data  [datamining_helper](eanalytics_api_py/conn.py)

### 0.0.64
Fix indexing issue in [datamining_helper](eanalytics_api_py/datamining_helper.py)

### 0.0.63
New module: datamining_helper.py
New method: deduplicate_product_cols_file_2_df
See [here](eanalytics_api_py/datamining_helper.py)