import requests
import os
import time
import gzip
from pprint import pprint
import ijson
import json
import csv
import inspect

class Conn:
    """Setup the connexion to Eulerian Technologies API.

        Parameters
        ----------
        gridpool_name : str, obligatory
            Your assigned grid in Eulerian Technologies platform

        datacenter : str, obligatory
            Your assigned datacenter (com for Europe, ca for Canada) in Eulerian Technologies platform

        api_key : str, obligatory
            Your Eulerian Technologies user account API key

        pprint_log : bool, optional
            If set to False, will not print log messages
            Default: True

    Returns
    -------
        Class is instantiated
    """
    def __init__(
            self,
            gridpool_name : str,
            datacenter  : str,
            api_key : str,
            print_log = True
    ):
        self.__print_log = print_log
        self.__http_headers = { 'Authorization' : 'Bearer %s'%(api_key) }
        self.__gridpool_name = gridpool_name
        self.__base_url = 'https://%s.api.eulerian.%s'%(self.__gridpool_name, datacenter)
        overview_url = self.__base_url+'/ea/v2/er/account/authtree.json'
        overview_json = requests.get(overview_url, headers=self.__http_headers).json()
        if self.__has_api_error(overview_json):
            raise Exception("Connexion unsuccessful for:\n"\
                                +"\tgridpool_name=%s\n"%(self.__gridpool_name) \
                                +"\tdatacenter=%s\n"%(datacenter) \
                                +"\tapi_key=%s\n"%(api_key)
            )
        else:
            self.__log("Connexion ok")

    def __skipping_download(self, output_path2file, override_file):
        if os.path.exists(output_path2file) and not override_file:
            self.__log(f'Output_path2file={output_path2file} already exists, skipping download')
            return 1
        elif os.path.exists(output_path2file) and override_file:
            self.__log(f'Local path2file={output_path2file} will be overridden with new data')
            return 0

    def __create_output_directory(self, output_directory):
        if output_directory and not os.path.exists(output_directory):
            try:
                os.mkdir(output_directory)
            except Exception as e:
                print(e)
                return 1
        return 0

    def __log(self, log):
        """ A simple logging mechanism """
        if self.__print_log:
            stack = inspect.stack()
            frame = stack[1]
            caller_func = frame.function
            caller_mod = inspect.getmodule(frame[0])
            log_msg = '%s:%s:%s: %s' %(time.ctime(), caller_mod.__name__, caller_func, log)
            print(log_msg)


    def __has_api_error(self, _json):
        """ Check for error in the JSON returned by Eulerian Technologies API """
        if 'error' in _json.keys() and _json['error'] \
        or 'success' in _json.keys() and not _json['success'] \
        or 'status' in _json.keys() and _json['status'].lower() == 'failed':
            pprint(_json)
            return 1
        return 0

    def download_datamining(
            self,
            website_name: str,
            datamining_type: str,
            jobrun_id = None,
            payload = {},
            output_directory = None,
            output_filename = None,
            override_file = False,
    ):

        """ Fetch datamining data from the API into a gzip compressed file

            Parameters
            ----------
            website_name : str, obligatory
                Your targeted website_name in Eulerian Technologies platform

            datamining_type : str, obligatory
                The targeted datamining (isenginerequest, actionlogorder, scart ,estimate, order)

            jobrun_id : str, optional
                The jobrun_id to download directly from a previously requested jobrun

            payload : dict, optional
                The datamining payload that contains the requested data

            output_directory : str, optional
                The local targeted  directory

            output_filename: str, optional
                If not set, the file will be created in the current working directory with a default name

            override_file : bool, optional
                If set to True, will override output_path2file (if exists) with the new datamining content
                Default: False

        Returns
        -------
        str
            The output_path2file containing the downloaded datamining data
        """
        if not output_directory:
            output_directory = ''
        if self.__create_output_directory(output_directory): # error
            return 0
        if not output_filename:
            date_from = payload['date-from'].replace('/', '_')
            date_to = payload['date-from'].replace('/', '_')
            output_filename = '_'.join([website_name, datamining_type, date_from, date_to])
            output_filename += '.csv.gz'
        output_path2file = os.path.join(output_directory, output_filename)

        if self.__skipping_download(output_path2file, override_file):
            return output_path2file

        if jobrun_id:
            self.__log('Fetching data from jobrun_id=%s' %(jobrun_id))
        else:
            search_url = self.__base_url+'/ea/v2/ea/%s/report/%s/search.json'%(website_name, datamining_type)
            search_json = requests.get(search_url, params=payload, headers=self.__http_headers).json()
            if self.__has_api_error(search_json):
                return 0

            jobrun_id = search_json['jobrun_id']
            status_url =  self.__base_url+'/ea/v2/ea/%s/report/%s/status.json'%(website_name, datamining_type)
            status_payload = { 'jobrun-id' : jobrun_id }
            ready = False
            self.__log(f'Waiting for jobrun_id={jobrun_id} to complete')
            while not ready:
                time.sleep(5)
                status_json = requests.get(status_url, params=status_payload, headers=self.__http_headers).json()
                if self.__has_api_error(status_json):
                    return 0
                if status_json['jobrun_status'] == 'COMPLETED':
                    ready = True

        self.__log('Downloading data')
        download_url = self.__base_url+'/ea/v2/ea/%s/report/%s/download.json'%(website_name, datamining_type)
        download_payload = { 'output-as-csv' : 1, 'jobrun-id' : jobrun_id }
        with  gzip.open(output_path2file, 'wb') as f:
            r = requests.get(
                    download_url,
                    params = download_payload,
                    headers = self.__http_headers,
                    stream=True
            )
            for chunk in r.iter_content(1024*1024*5): # 5MB
                f.write(chunk)

        self.__log('Data downloaded, path2file=%s'%(output_path2file))
        return output_path2file


    def download_edw(
            self,
            query : str,
            ip = None,
            token_path2file = None,
            force_token_refresh = False,
            output_directory = None,
            output_filename = None,
            override_file = False,
            jobrun_id = None,
    ):
        """ Fetch edw data from the API into a gzip compressed file

            Parameters
            ----------
            query: str, obligatory
                EDW query

            ip: str, optional
                Coma separated ip values
                Default: Automatically fetch your external ip address

            token_path2file: str, optional
                Path2file where the edw access token will be locally stored
                Default: current working directory edw_token.json

            force_token_refresh: str, optional
                Will fetch a new edw access token regardless of the current token validity
                Default: False

            output_directory : str, optional
                The local targeted  directory

            output_filename: str, optional
                If not set, the file will be created in the current working directory with a default name

            override_file : bool, optional
                If set to True, will override output_path2file (if exists) with the new datamining content
                Default: False

            jobrun_id : str, optional
                The jobrun_id to download directly from a previously requested jobrun

        Returns
        -------
        str
            The output_path2file containing the downloaded datamining data
        """


        def save_get_edw_token():
            token_expiry_epoch = int(time.time()) + 3600 * 11.5
            edw_token_url = self.__base_url+'/ea/v2/er/account/get_dw_session_token.json'
            edw_token_json = requests.get(edw_token_url, headers=self.__http_headers, params={'ip' : ip}).json()
            
            if self.__has_api_error(edw_token_json):
                return 0

            edw_token = edw_token_json['data']['rows'][0][0]
            token_json = { 'token' : edw_token, 'expiry' : token_expiry_epoch, 'ip' : ip }
            with open(token_path2file, 'w') as f:
                json.dump(token_json, f, ensure_ascii=False, indent=4)
            return edw_token
        
        def conditional_token_refresh():
            if force_token_refresh:
                self.__log('Forcing edw token refresh')
                return save_get_edw_token()

            if not os.path.exists(token_path2file):
                self.__log('No Local edw token file detected, fetching a new one')
                return save_get_edw_token()

            with open(token_path2file) as f:
                edw_token_json = json.load(f)
                current_epoch = int(time.time())
                if not all (k in edw_token_json for k in ('token', 'expiry', 'ip')) \
                or ip != edw_token_json['ip'] \
                or int(edw_token_json['expiry'] < int(time.time())):
                    self.__log('Local edw token no longer valid, fetching a new one')
                    return save_get_edw_token()
                self.__log('Local edw token still valid, loaded from file=%s'%(token_path2file))
                return edw_token_json['token']
       
        if not output_directory:
            output_directory = ''     
        if self.__create_output_directory(output_directory): # error
            return 0
        if not output_filename:
            output_filename = '_'.join([self.__gridpool_name, 'dw', time.time()])
            output_filename += 'csv.gz'
        output_path2file = os.path.join(output_directory, output_filename)
        output_path2file_temp = output_path2file+'.temp'

        if self.__skipping_download(output_path2file, override_file):
            return output_path2file

        if  token_path2file:
            if self.__create_output_directory(os.path.split(token_path2file)[0]): # error
                return 0
        else:
            token_path2file = 'edw_token.json'

        if not ip:
            ip = requests.get('https://api.ipify.org').text

        edw_token = conditional_token_refresh()
        edw_http_headers =  { 
            'Authorization' : 'Bearer %s'%(edw_token),
            'Content-Type' : 'application/json'
        }

        if jobrun_id:
            self.__log('Fetching file from jobrun_id=%s' %(jobrun_id))
        else:
            search_url =  self.__base_url+':981/edw/jobs'
            self.__log(search_url)
            edw_json = {
                "kind" : "edw#request",
                "query" : query
            }
            search_json = requests.post(
                        search_url,
                        json=edw_json,
                        headers=edw_http_headers,
            ).json()
            if self.__has_api_error(search_json):
                return 0
            jobrun_id = search_json['data'][0]
        status_url =  self.__base_url+':981/edw/jobs/'+str(jobrun_id)

        ready = False
        self.__log(f'Waiting for jobrun_id={jobrun_id} to complete')
        while not ready:
            time.sleep(5)
            status_json = requests.get(status_url, headers=edw_http_headers).json()
            if self.__has_api_error(status_json):
                return 0
            if status_json['status'] == 'Done':
                ready = True
                download_url = status_json['data'][1]
        
        with open(output_path2file_temp, 'wb') as f:
            r = requests.get(
                    download_url,
                    headers = self.__http_headers,
                    stream=True
            )
            for chunk in r.iter_content(1024*1024*5): # 5MB
                f.write(chunk)

        self.__log('JSON data downloaded into path2file=%s'%(output_path2file_temp))
        self.__log('Converting JSON to CSV...')

        with gzip.open(output_path2file, 'wt') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=';')

            with open(output_path2file_temp) as f:
                headers_object = ijson.items(f, 'headers.schema')
                for headers in headers_object:
                    csv_headers = [header[1] for header in headers]
                    csvwriter.writerow(csv_headers)
              
            with open(output_path2file_temp) as f:
                rows_object = ijson.items(f, 'rows')
                for rows in rows_object:
                    for row in rows:
                        csvwriter.writerow(row)

            # kill job
            try:
                with open(output_path2file_temp) as f:
                    uuid_object = ijson.items(f, 'headers.uuid')
                    for uuid in uuid_object:
                        edw_json['query'] = 'KILL %s;'%(uuid)
                        requests.post(search_url, headers=edw_http_headers, json=edw_json).json()
            except Exception as e:
                print(e)
                print('Could not kill the process uuid=%s'%(uuid))

        # removing temp file
        if os.path.exists(output_path2file_temp):
            self.__log('Deleting temp_file=%s'%(output_path2file_temp))
            os.remove(output_path2file_temp)
        self.__log('Output csv path2file=%s'%(output_path2file))

        return output_path2file