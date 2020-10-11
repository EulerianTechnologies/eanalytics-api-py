import requests
import os
import time
import gzip
from pprint import pprint
import ijson
import json
import csv
import inspect
import urllib
from datetime import datetime, timedelta
import re
import pandas as pd

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

    print_log : bool, optional
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
        self.__gridpool_name = gridpool_name
        self.__http_headers = { "Authorization" : f"Bearer {api_key}" }
        self.__base_url = f"https://{gridpool_name}.api.eulerian.{datacenter}"
        self.__api_key = api_key

        overview_url = f"{self.__base_url}/ea/v2/er/account/authtree.json"
        overview_json = requests.get(
            url=overview_url, 
            headers=self.__http_headers
        ).json()

        if self.__has_api_error(overview_json):
            raise SystemError(f"Error for url={overview_url}")
        else:
            self.__log("Connexion ok")

    def __skipping_download(self, output_path2file, override_file):
        """ Skip download if local file exists and not override_file """

        if os.path.exists(output_path2file) and not override_file:
            self.__log(f'Output_path2file={output_path2file} already exists, skipping download')
            return 1

        elif os.path.exists(output_path2file) and override_file:
            self.__log(f'Local path2file={output_path2file} will be overridden with new data')
            return 0

        else:
            self.__log(f'path2file={output_path2file} not found, downloading new data')
            return 0

    def __log(self, log):
        """ A simple logging mechanism """

        if self.__print_log:
            stack = inspect.stack()
            frame = stack[1]
            caller_func = frame.function
            caller_mod = inspect.getmodule(frame[0])
            log_msg = f"{time.ctime()}:{caller_mod.__name__}:{caller_func}: {log}"
            print(log_msg)

    def __has_api_error(self, _json):
        """ Check for error in the JSON returned by Eulerian Technologies API """

        if "error" in _json.keys() and _json["error"] \
        or "success" in _json.keys() and not _json["success"] \
        or "status" in _json.keys() and _json['status'].lower() == "failed":
            self.__log("Error from Eulerian Technologies API")
            pprint(_json)
            return 1
        return 0

    def download_realtime_report(
            self,
            website_name: list,
            report_name: list,
            payload : dict,
    ):

        """ Fetch realtime report data into a pandas dataframe

        Parameters
        ----------
        website_name : str, mandatory
            Your targeted website_name in Eulerian Technologies platform

        report_name: str, mandatory

        payload : dict, mandatory
            The realtime report payload

        Returns
        -------
        pd.DataFrame()
            A pandas dataframe
        """

        if not isinstance(website_name, str):
            raise TypeError("website_name should be either a string")

        if not isinstance(report_name, str):
            raise TypeError("report_name should be a string")

        report_url = f"{self.__base_url}/ea/v2/ea/{website_name}/report/realtime/{report_name}.json"
        payload['ea-switch-datetorow'] = 1
        payload['ea-enable-datefmt'] = "%s"
  
        report_json = requests.get(
            url=report_url,
            params=payload,
            headers=self.__http_headers,
        ).json()

        if self.__has_api_error(report_json):
            raise SystemError(f"Error for url={report_url}?{urllib.parse.urlencode(payload)}")

        fields = [ field['name'] for field in report_json['data']['fields'] ]
        rows =  report_json['data']['rows']

        df = pd.DataFrame(
            columns=fields,
            data=rows,
        )

        df["website"] = website_name
        df["date"] = pd.to_datetime(df["date"], unit='s')
        if 'id' in df.columns:
            df = df.drop('id', axis=1)

        return df

    def download_datamining(
            self,
            website_name: str,
            datamining_type: str,
            jobrun_id = None,
            payload = {},
            status_waiting_seconds = 5,
            output_directory = None,
            override_file = False,
            n_days_slice = 31,
    ):

        """ Fetch datamining data from the API into a gzip compressed file

        Parameters
        ----------
        website_name : str, obligatory
            Your targeted website_name in Eulerian Technologies platform

        datamining_type : str, obligatory
            The targeted datamining (isenginerequest, actionlogorder, scart ,estimate, order)

        payload : dict, optional
            The datamining payload that contains the requested data

        status_waiting_seconds: int, optional
            Waiting time in seconds between each status query

        output_directory : str, optional
            The local targeted  directory

        override_file : bool, optional
            If set to True, will override output_path2file (if exists) with the new datamining content
            Default: False

        n_days_slice: int, optional
            Split datamining query into days slice to reduce server load
            Default: 31

        Returns
        -------
        list
            A list of path2file
        """

        def download (
            website_name: str,
            datamining_type: str,
            payload: dict,
            date_format : str,
            dt_date_from: timedelta(),
            dt_date_to: timedelta(),
            view_id: int,
            status_waiting_seconds: int,
        ):
            date_from = dt_date_from.strftime(date_format)
            date_to = dt_date_to.strftime(date_format)
            date_from_file = date_from.replace("/", "_")
            date_to_file = date_to.replace("/", "_")

            output_filename = f"{website_name}_{datamining_type}_view_{view_id}_from_{date_from_file}_to_{date_to_file}.csv.gz"
            output_path2file = os.path.join(output_directory, output_filename)

            if self.__skipping_download(output_path2file, override_file):
                return output_path2file

            payload['date-from'] = date_from
            payload['date-to'] = date_to

            search_url = f"{self.__base_url}/ea/v2/ea/{website_name}/report/{datamining_type}/search.json"
            search_url_debug = f"{self.__base_url}/ea/v2/{self.__api_key}/ea/{website_name}/report/{datamining_type}/search.json"
            self.__log(f"http get url {search_url_debug}?{urllib.parse.urlencode(payload, safe='/')}")

            search_json = requests.get(
                url=search_url,
                params=payload,
                headers=self.__http_headers
            ).json()

            if self.__has_api_error(search_json):
                raise SystemError(f"Error for url={search_url}?{urllib.parse.urlencode(payload)}")

            jobrun_id = search_json["jobrun_id"]

            status_url =  f"{self.__base_url}/ea/v2/ea/{website_name}/report/{datamining_type}/status.json"
            status_payload = { "jobrun-id" : jobrun_id }
            ready = False

            if status_waiting_seconds < 5 or not isinstance(status_waiting_seconds, int):
                status_waiting_seconds = 5

            while not ready:
                self.__log(f'Waiting for jobrun_id={jobrun_id} to complete')
                time.sleep(status_waiting_seconds)
                status_json = requests.get(
                    url=status_url,
                    params=status_payload,
                    headers=self.__http_headers
                ).json()

                if self.__has_api_error(status_json):
                    raise SystemError(f"Error for url={status_url}?{urllib.parse.urlencode(status_payload)}")

                if status_json["jobrun_status"] == "COMPLETED":
                    ready = True

            self.__log('Downloading data')
            download_url = f"{self.__base_url}/ea/v2/ea/{website_name}/report/{datamining_type}/download.json"
            download_payload = { 'output-as-csv' : 0, 'jobrun-id' : jobrun_id }
            output_path2file_temp = f"{output_path2file}.tmp"

            with  open(output_path2file_temp, 'wb') as f:
                r = requests.get(
                        download_url,
                        params = download_payload,
                        headers = self.__http_headers,
                        stream=True
                )
                try:
                    self.__log(f"Content-Length is {int(r.headers['Content-Length'])/(1024*1024)} MBs")

                except Exception as e:
                    self.__log("Could not read Content-Length header")

                for chunk in r.iter_content(1024 * 1024 * 5): # 5MB
                    f.write(chunk)

            self.__log(f"JSON data downloaded into path2file={output_path2file_temp}")
            self.__log("Converting JSON to CSV...")

            with gzip.open(output_path2file, "wt") as csvfile:
                csvwriter = csv.writer(csvfile, delimiter=';')

                with open(output_path2file_temp) as f:
                    columns = []
                    headers_object = ijson.items(f, "data.fields")
                    for headers in headers_object:
                        # header are too messy to work with, working on internal name
                        for header in headers:
                            # prdparam case
                            if header["name"].startswith('productparam'):
                                match = re.search(
                                    pattern = r'\s:\s([\w\W]+?)\s#\s(\d)+$',
                                    string = header["header"]
                                )
                                prdp_name = match.group(1)
                                prd_idx = int(match.group(2))-1 # start at 0
                                header["name"] = f"productparam_{prdp_name}_{prd_idx}"
                            # cgip case
                            if header["name"].startswith('cgiparam'):
                                match = re.search(
                                    pattern = r'[\w\W]+?\s:\s(.*)$',
                                    string = header["header"],
                                )
                                cgip_name = match.group(1)
                                header["name"] = f"cgiparam_{cgip_name}"
                            columns.append(header["name"])
                        csvwriter.writerow(columns)
                  
                with open(output_path2file_temp) as f:
                    rows_object = ijson.items(f, "data.rows")
                    for rows in rows_object:
                        for row in rows:
                            csvwriter.writerow(row)

            # removing temp file
            if os.path.exists(output_path2file_temp):
                self.__log(f"Deleting temp_file={output_path2file_temp}")
                os.remove(output_path2file_temp)

            self.__log(f"Output csv path2file={output_path2file}")
            return output_path2file

        l_path2file = [] # store each file for n_days_slice
        l_allowed_datamining_types = ["order", "estimate", "isenginerequest", "actionlog", "scart"]
        date_format = "%m/%d/%Y"

        if datamining_type not in l_allowed_datamining_types:
            raise ValueError(f"datamining_type={datamining_type} not allowed, use one of the following: {', '.join(l_allowed_datamining_types)}")

        if not isinstance(n_days_slice, int):
            raise TypeError(f"type of 'n_days_slice' expected: 'int' but got: '{type(n_days_slice)}'")

        date_from = payload["date-from"] if "date-from" in payload else None
        if not date_from:
            raise ValueError("missing parameter=date-from in payload object")

        date_to = payload['date-to'] if 'date-to' in payload else None
        if not date_to:
            raise ValueError("missing parameter=date-from in payload object")

        n_days_slice = timedelta(days=n_days_slice)
        one_day_slice = timedelta(days=1)

        dt_date_from = datetime.strptime(date_from, date_format)
        dt_date_to = datetime.strptime(date_to, date_format)

        if dt_date_from > dt_date_to:
            raise ValueError("'date-from' cannot occur later than 'date-to'")

        if not output_directory:
            output_directory = ""
        elif not os.path.exists(output_directory):
            os.mkdir(output_directory)

        view_id = int(payload["view-id"]) if "view-id" in payload else 0

        while dt_date_from + n_days_slice <= dt_date_to:
            dt_tmp_date_to = dt_date_from + n_days_slice
            output_path2file = download(
                website_name = website_name,
                datamining_type = datamining_type,
                payload = payload,
                dt_date_from = dt_date_from,
                dt_date_to = dt_tmp_date_to,
                status_waiting_seconds = status_waiting_seconds,
                view_id = view_id,
                date_format = date_format,
            )
            l_path2file.append(output_path2file)
            dt_date_from += n_days_slice + one_day_slice

        else:
            if dt_date_from > dt_date_to and len(l_path2file): 
                pass

            else: # last slice
                output_path2file = download(
                    website_name = website_name,
                    datamining_type = datamining_type,
                    payload = payload,
                    dt_date_from = dt_date_from,
                    dt_date_to = dt_date_to,
                    status_waiting_seconds = status_waiting_seconds,
                    view_id = view_id,
                    date_format = date_format,
                )
                l_path2file.append(output_path2file)

        return l_path2file

    def download_edw(
            self,
            query : str,
            status_waiting_seconds=5,
            ip = None,
            output_path2file = None,
            override_file = False,
            jobrun_id = None,
    ):
        """ Fetch edw data from the API into a gzip compressed file

        Parameters
        ----------
        query: str, obligatory
            EDW query

         status_waiting_seconds: int, optional
            Waiting time in seconds between each status query

        ip: str, optional
            Coma separated ip values
            Default: Automatically fetch your external ip address

        output_path2file: str, optional
            path2file where the data will be stored
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

        if not isinstance(query, str):
            raise TypeError("query should be a string")

        epoch_from_to_findall = re.findall(
            r'{\W+?(\d+)\W+?(\d+)\W+?}',
            query
        )      
        if not epoch_from_to_findall:
            raise ValueError(f"Could not read epoch_from and epoch_to from query=\n{query}")

        readers_findall = re.findall(r'\w+:\w+@[\w_-]+', query)
        readers = list(
            map(
                lambda s: s.replace(':', '_').replace('@', '_'),
                readers_findall
            )
        )
        if not readers:
            raise ValueError(f"Could not read READER from query=\n{query}")

        if not output_path2file:
            output_path2file = ''.join([
                "dw_",
                self.__gridpool_name+"_",
                "_".join( epoch_from_to_findall[0]),
                "_".join(readers)+"_",
                ".csv.gzip"
            ])

        if self.__skipping_download(output_path2file, override_file):
            return output_path2file

        output_path2file_temp = f"{output_path2file}.tmp"

        if not ip:
            self.__log(f"No ip provided\
                \n Fetching external ip from https://api.ipify.org\
                \nif using a vpn, please provide the vpn ip\
            ")
            ip = requests.get(url="https://api.ipify.org").text

        edw_token_url = f"{self.__base_url}/ea/v2/er/account/get_dw_session_token.json"
        payload = { 'ip' : ip }

        edw_token_json = requests.get(
            url=edw_token_url,
            headers=self.__http_headers,
            params=payload
        ).json()

        if self.__has_api_error(edw_token_json):
            raise SystemError(f"Error for url={edw_token_url}?{urllib.parse.urlencode(payload)}")

        edw_token = edw_token_json["data"]["rows"][0][0]

        edw_http_headers =  { 
            "Authorization" : "Bearer "+edw_token,
            "Content-Type" : "application/json"
        }

        if not jobrun_id:
            search_url =  f"{self.__base_url}:981/edw/jobs"
            self.__log(search_url)

            edw_json_params = {
                "kind" : "edw#request",
                "query" : query
            }

            search_json = requests.post(
                search_url,
                json=edw_json_params,
                headers=edw_http_headers,
            ).json()


            if self.__has_api_error(search_json):
                pprint(edw_json_params)
                raise SystemError(f"Error for url={search_url}")

            jobrun_id = search_json['data'][0]

        status_url =  f"{self.__base_url}:981/edw/jobs/{jobrun_id}"

        if status_waiting_seconds < 5 or not isinstance(status_waiting_seconds, int):
            status_waiting_seconds = 5

        ready = False

        while not ready:
            self.__log(f"Waiting for jobrun_id={jobrun_id} to complete")
            time.sleep(status_waiting_seconds)
            status_json = requests.get(
                url=status_url,
                headers=edw_http_headers
            ).json()

            if self.__has_api_error(status_json):
                raise SystemError(f"Error for url={status_url}")

            if status_json["status"] == "Done":
                ready = True
                download_url = status_json["data"][1]
        
        with open(output_path2file_temp, "wb") as f:
            r = requests.get(
                    download_url,
                    headers = self.__http_headers,
                    stream=True
            )
            try:
                self.__log(f"Content-Length is {int(r.headers['Content-Length'])/(1024*1024)} MBs")

            except Exception as e:
                self.__log("Could not read Content-Length http header in response")

            for chunk in r.iter_content(1024*1024*5): # 5MB
                f.write(chunk)

        self.__log(f"JSON data downloaded into path2file={output_path2file_temp}")
        self.__log("Converting JSON to CSV...")

        with gzip.open(output_path2file, "wt") as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=';')

            with open(output_path2file_temp) as f:
                headers_object = ijson.items(f, "headers.schema")
                for headers in headers_object:
                    csv_headers = [header[1] for header in headers]
                    csvwriter.writerow(csv_headers)
              
            with open(output_path2file_temp) as f:
                rows_object = ijson.items(f, "rows")
                for rows in rows_object:
                    for row in rows:
                        csvwriter.writerow(row)

            # kill job
            try:
                with open(output_path2file_temp) as f:
                    uuid_object = ijson.items(f, "headers.uuid")
                    for uuid in uuid_object:
                        cancel_url = f"{search_url}/{uuid}/cancel"
                        requests.post(
                            url=cancel_url,
                            headers=edw_http_headers
                        ).json()

            except Exception as e:
                print(e)
                print(f"Could not kill the process uuid={uuid}")

        # removing temp file
        if os.path.exists(output_path2file_temp):
            self.__log(f"Deleting temp_file={output_path2file_temp}")
            os.remove(output_path2file_temp)

        self.__log(f"Output csv path2file={output_path2file}")

        return output_path2file

    def get_view_id_name_map (
        self,
        website_name : str
    ) -> dict:
        """ Fetch attribution rules

        Parameters
        ----------
        website_name: str, obligatory
            Your targeted website_name in Eulerian Technologies platform

        Returns
        -------
        dict
            A dict as { "view_id" : "view_name", ...}
        """

        view_url = f"{self.__base_url}/ea/v2/ea/{website_name}/db/view/get_all_name.json"
        view_json = requests.get(
            url=view_url,
            headers=self.__http_headers
        ).json()

        if self.__has_api_error(view_json):
            raise SystemError(f"Error for url={view_url}")

        view_id_idx = view_json["data"]["fields"].index({"name" : "view_id"})
        view_name_idx = view_json["data"]["fields"].index({"name" : "view_name"})
        views = { view[view_id_idx] : view[view_name_idx] for view in view_json["data"]["rows"] }

        if "0" not in views:
            views["0"] = "last channel"

        return views

    def get_website_by_name(
        self,
        website_name : str
    ) -> dict :
        """ Fetch attribution rules

        Parameters
        ----------
        website_name: str, obligatory
            Your targeted website_name in Eulerian Technologies platform

        Returns
        -------
        dict
            A dict as { "website_prop" : "website_prop_value" }
        """

        website_url = f"{self.__base_url}/ea/v2/ea/{website_name}/db/website/get_me.json"
        website_json =  requests.get(
            url=website_url,
            headers=self.__http_headers
        ).json()

        if self.__has_api_error(website_json):
            raise SystemError(f"Error for url={website_url}")

        return  {
             website_json["data"]["fields"][i]["name"] : website_json["data"]["rows"][0][i]
                for i in range(len(website_json["data"]["fields"]))
        }