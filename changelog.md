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
