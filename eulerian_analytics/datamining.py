from .helper import log, has_api_error
import requests
import os
import time
import gzip

def download(
        gridpool_name: str,
        datacenter: str,
        website_name: str,
        api_key: str,
        datamining_type: str,
        payload: dict,
        output_path2file = '',
        override_file = False,
) -> str:
    """Download a datamining dataset from Eulerian Technologies API into a gzip archive.

        Parameters
        ----------
        gridpool_name : str, obligatory
            Your assigned grid in Eulerian Technologies platform

        datacenter : str, obligatory
            Your assigned datacenter (com for Europe, ca for Canada) in Eulerian Technologies platform

        website_name : str, obligatory
            Your targeted website_name in Eulerian Technologies platform

        api_key : str, obligatory
            Your Eulerian Technologies user account API key

        datamining_type : str, obligatory
            The targeted datamining (order,estimate...)           

        payload : dict, obligatory
            The datamining payload that contains the requested data

        output_path2file : str, optional
            The targeted local path2file in which the data will be downloaded into
            If not set, the file will be created in the current working directory with a default name

        override_file : bool, optional
            If set to True, will override output_path2file (if exists) with the new datamining content
            Default: False

    Returns
    -------
    str
        the output_path2file containing the downloaded datamining data
    """

    splitted_output_path2file = output_path2file.split('/') 
    if len(splitted_output_path2file) > 1: # directory + filename
        output_dir = splitted_output_path2file[-1]

    elif splitted_output_path2file[0]: #filename
        output_dir = False

    else: # no path2file
        output_dir = False
        output_path2file = _build_output_path2file(website_name, datamining_type, payload)

    if output_dir and not os.path.exists(output_dir):
        os.mkdir(output_dir)

    if os.path.exists(output_path2file) and not override_file:
        log(f'output_path2file={output_path2file} already exists, skipping download')
        return output_path2file

    log(f'output_path2file={output_path2file}')
    base_url = 'https://%s.api.eulerian.%s/ea/v2/ea/%s/report/%s/' \
    %(gridpool_name, datacenter, website_name, datamining_type)
    http_headers = { 'Authorization' : f'Bearer {api_key}' }

    search_url = base_url + 'search.json'
    search_json = requests.get(search_url, params=payload, headers=http_headers).json()
    if has_api_error(search_json):
        return 0

    jobrun_id = search_json['jobrun_id']
    status_url =  base_url+'status.json'
    status_payload = { 'jobrun-id' : jobrun_id }
    ready = False
    log(f'Waiting for jobrun_id={jobrun_id} to complete')
    while not ready:
        time.sleep(5)
        status_json = requests.get(status_url, params=status_payload, headers=http_headers).json()
        if has_api_error(status_json):
            return 0
        if status_json['jobrun_status'] == 'COMPLETED':
            ready = True

    download_url = base_url+'download.json'
    download_payload = { 'output-as-csv' : 1, 'jobrun-id' : jobrun_id }

    log('Downloading data')
    with  gzip.open(output_path2file, 'wb') as f:
        r = requests.get(download_url, params=download_payload, headers=http_headers, stream=True)
        for chunk in r.iter_content(1024*1024*5): # 5MB
            f.write(chunk)

    log('Data downloaded')
    return output_path2file

def _build_output_path2file(website_name, datamining_type, payload):
    """ Internal helper function, build a path2file if not defined

    Parameters
    ----------
    website_name : str, obligatory
        Your targeted website_name in Eulerian Technologies platform

    datamining_type : str, obligatory
        The targeted datamining (order,estimate...)           

    payload : dict, obligatory
        The datamining payload that contains the requested data

    Returns
    -------
    str
        the output_path2file containing the downloaded datamining data
    """
    date_from = payload['date-from'].replace('/', '_')
    date_to = payload['date-from'].replace('/', '_')
    output_path2file = '_'.join([website_name, datamining_type, date_from, date_to])
    output_path2file += '.csv.gz'
    return output_path2file