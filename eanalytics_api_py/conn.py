""" This module contains a Conn class to connect and
retrive data from Eulerian Technologies API
"""

import os
import time
import gzip
import re
import urllib
import csv
from datetime import datetime, timedelta
import logging

import pandas as pd
import requests
import ijson

from .internal.os import _is_skippable_request
from .internal.request import _request_to_json

# logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# stream handler
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s:%(name)s:%(funcName)s:%(levelname)s: %(message)s')
ch.setFormatter(formatter)

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

    log_level : str, optional
        Log level accepted values: ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        Default: INFO

    Returns
    -------
        Class is instantiated
    """
    def __init__(
        self,
        gridpool_name : str,
        datacenter  : str,
        api_key : str,
        log_level : str = "INFO"
    ):
        if not isinstance(log_level, str):
            raise TypeError("log_level should be a string type")

        allowed_log_level = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        log_level = log_level.upper()
        if log_level not in allowed_log_level:
            raise ValueError(f"Accepted log_level values= {', '.join(allowed_log_level)}")

        ch.setLevel(log_level)
        logger.addHandler(ch)

        if not isinstance(gridpool_name, str):
            raise TypeError("gridpool_name should be a string type")
        self.__gridpool_name = gridpool_name

        if not isinstance(datacenter, str):
            raise TypeError("datacenter should be a string type")
        self.__base_url = f"https://{gridpool_name}.api.eulerian.{datacenter}"
        self.__api_v2 = f"{self.__base_url}/ea/v2"

        if not isinstance(api_key, str):
            raise TypeError("api_key should be a string type")

        self.__api_key = api_key
        self.__http_headers = { "Authorization" : f"Bearer {api_key}" }

        self._check_credentials()
        logger.info("Credentials ok")

    def _check_credentials(self) -> None:
        """Check credentials validity"""
        overview_url = f"{self.__api_v2}/er/account/authtree.json"

        # raise an error if API error or fail to load as json
        _request_to_json(
            request_type = "get",
            url=overview_url,
            headers=self.__http_headers,
            logger=logger
        )

    def download_realtime_report(
        self,
        website_name: str,
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
            raise TypeError("website_name should be a str type")

        if not isinstance(report_name, str):
            raise TypeError("report_name should be a str typoe")


        if not isinstance(payload, dict):
            raise TypeError("payload should be a dict typoe")

        if not payload:
            raise ValueError("payload should not be empty")

        report_url = f"{self.__api_v2}/ea/{website_name}/report/realtime/{report_name}.json"
        payload['ea-switch-datetorow'] = 1 # include the date in each row
        payload['ea-enable-datefmt'] = "%s" # format the date as an epoch timestamp

        report_json = _request_to_json(
            request_type="get",
            url=report_url,
            params=payload,
            headers=self.__http_headers,
            logger=logger
        )

        fields = [ field['name'] for field in report_json['data']['fields'] ]
        rows =  report_json['data']['rows']

        df = pd.DataFrame(
            columns=fields,
            data=rows,
        )

        df["website"] = website_name # useful when querying multiple websites will be implemented
        df["date"] = pd.to_datetime(df["date"], unit='s')

        return df

    def download_datamining(
        self,
        website_name: str,
        datamining_type: str,
        payload = None,
        status_waiting_seconds = 5,
        output_directory = None,
        override_file = False,
        n_days_slice = 31,
    ):

        """ Fetch datamining data from the API into a gzip compressed CSV file

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
            If set to True, will override output_path2file (if exists)
                with the new datamining content
            Default: False

        n_days_slice: int, optional
            Split datamining query into days slice to reduce server load
            Default: 31

        Returns
        -------
        list
            A list of path2file
        """

        # inner function
        def download (
            website_name: str,
            datamining_type: str,
            payload: dict,
            date_format : str,
            dt_date_from: timedelta(),
            dt_date_to: timedelta(),
            status_waiting_seconds: int,
        ) -> str :
            """ download locally JSON response from API and convert it into a gzipped CSV file """
            date_from = dt_date_from.strftime(date_format)
            date_to = dt_date_to.strftime(date_format)
            date_from_file = date_from.replace("/", "_")
            date_to_file = date_to.replace("/", "_")

            output_filename = "_".join([
                website_name,
                datamining_type,
                "view",
                payload["view-id"],
                "from",
                date_from_file,
                "to",
                date_to_file,
            ])+".csv.gz"
            output_path2file = os.path.join(output_directory, output_filename)

            if _is_skippable_request(
                output_path2file=output_path2file,
                override_file=override_file,
                logger=logger
            ):
                return output_path2file

            payload['date-from'] = date_from
            payload['date-to'] = date_to

            search_url = f"{self.__api_v2}/ea/{website_name}/report/{datamining_type}/search.json"
            search_url_debug = f"{self.__api_v2}/{self.__api_key}/ea/{website_name}/report/{datamining_type}/search.json"
            logger.info(f"search url {search_url_debug}?{urllib.parse.urlencode(payload, safe='/')}")

            search_json = _request_to_json(
                request_type="get",
                url=search_url,
                params=payload,
                headers=self.__http_headers,
                logger=logger
            )

            jobrun_id = search_json["jobrun_id"]

            status_url =  f"{self.__api_v2}/ea/{website_name}/report/{datamining_type}/status.json"
            status_payload = { "jobrun-id" : jobrun_id }
            ready = False

            if not isinstance(status_waiting_seconds, int) or status_waiting_seconds < 5:
                status_waiting_seconds = 5

            while not ready:
                logger.info(f'Waiting for jobrun_id={jobrun_id} to complete')
                time.sleep(status_waiting_seconds)
                status_json = _request_to_json(
                    request_type="get",
                    url=status_url,
                    params=status_payload,
                    headers=self.__http_headers,
                    logger=logger
                )

                if status_json["jobrun_status"] == "COMPLETED":
                    ready = True

            logger.info('Downloading data')
            download_url = f"{self.__api_v2}/ea/{website_name}/report/{datamining_type}/download.json"
            download_payload = { 'output-as-csv' : 0, 'jobrun-id' : jobrun_id }

            download_url_debug = f"{self.__api_v2}/{self.__api_key}/ea/{website_name}/report/{datamining_type}/download.json"
            logger.info(f"download url {download_url_debug}?{urllib.parse.urlencode(download_payload, safe='/')}")


            req = urllib.request.Request(
                    url=f"{download_url}?{urllib.parse.urlencode(download_payload)}",
                    headers = self.__http_headers,
            )

            if req.has_header('Content-Length'):
                logger.info(f"Content-Length={int(req.get_header('Content-Length'))/(1024*1024):.2f} MBs")

            with gzip.open(
                filename=output_path2file,
                mode="wt"
            ) as csvfile:

                csvwriter = csv.writer(
                    csvfile,
                    delimiter=';'
                )

                with urllib.request.urlopen(req) as f:
                    columns = []
                    objects = ijson.items(f, "data.fields.item") #.item is for ijson
                    headers = (header for header in objects)
                    # working on header.name rather than header.header for consitency
                    # because the latest is language specific
                    for header in headers:
                        # this allows to recover the name of the product param name
                        # instead of the id
                        if header["name"].startswith('productparam_'):
                            match = re.search(
                                pattern = r'\s:\s([\w\W]+?)\s#\s(\d+)$',
                                string = header["header"]
                            )
                            prdp_name = match.group(1)
                            prd_idx = int(match.group(2))-1 # start at 0
                            header["name"] = f"productparam_{prdp_name}_{prd_idx}"
                        # this allows to recover the name of the cgi param name
                        # instead of the id
                        if header["name"].startswith('cgiparam_'):
                            match = re.search(
                                pattern = r'^[\w\W]+?\s:\s(.*)$',
                                string = header["header"],
                            )
                            cgip_name = match.group(1)
                            header["name"] = f"cgiparam_{cgip_name}"
                        # this allows to recover the name of the CRM param name
                        # instead of the id
                        if header["name"].startswith('iduserparam_'):
                            match = re.search(
                                pattern = r'^[\w\W]+?\s:\s(.*)$',
                                string = header["header"],
                            )
                            iduserparam_name = match.group(1)
                            header["name"] = f"iduserparam_{iduserparam_name}"
                        # this allows to recover the name of the audience name
                        # instead of the id
                        if header["name"].startswith('cluster_'):
                            match = re.search(
                                pattern = r'^[\w\W]+?\s:\s(.*)$',
                                string = header["header"],
                            )
                            cluster_name = match.group(1)
                            header["name"] = f"cluster_{cluster_name}"

                        columns.append(header["name"])
                    csvwriter.writerow(columns)

                with urllib.request.urlopen(req) as f:
                    objects = ijson.items(f, "data.rows.item") # .item is for ijson
                    rows = (row for row in objects)
                    for row in rows:
                        csvwriter.writerow(row)

            logger.info(f"Output csv path2file={output_path2file}")
            return output_path2file
            # end of inner function

        l_path2file = [] # store each file for n_days_slice
        l_allowed_datamining_types = ["order", "estimate", "isenginerequest", "actionlog", "scart"]
        date_format = "%m/%d/%Y"

        if not isinstance(website_name, str):
            raise TypeError("website_name should be a str type")

        if datamining_type not in l_allowed_datamining_types:
            raise ValueError(f"datamining_type={datamining_type} not allowed.\n\
                            Use one of the following: {', '.join(l_allowed_datamining_types)}")

        if not isinstance(n_days_slice, int) or n_days_slice < 0 :
            raise TypeError("n_days_slice should be a positive integer")

        if not isinstance(payload, dict) or not payload:
            raise TypeError("payload should be a non-empty dict")

        if output_directory:
            if not isinstance(output_directory, str):
                raise TypeError("output_directory should be a str type")

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
        dt_tmp_date_to = dt_date_to

        if dt_date_from > dt_date_to:
            raise ValueError("'date-from' cannot occur later than 'date-to'")

        if not output_directory:
            output_directory = ""
        elif not os.path.exists(output_directory):
            os.mkdir(output_directory)

        # marketing attribution rule id, default to 0
        if "view-id" in payload:
            payload["view-id"] = str(payload["view-id"])
            match = re.match(
                pattern=r'^[0-9]$',
                string=payload["view-id"]
            )
            if not match:
                raise ValueError("view-id should match ^[0-9]$")

        else:
            payload["view-id"] = "0"

        # To avoid overloading the API with huge requests
        # We split the requests into smallers timerantes "n_days_slice"
        # While tempory date to delta is smaller than requested time to delta
        while dt_tmp_date_to <= dt_date_to:
            dt_tmp_date_to = dt_date_from + n_days_slice
            # Cannot request further than dt_date_to
            dt_tmp_date_to = dt_date_to if dt_tmp_date_to >= dt_date_to else  dt_tmp_date_to
            output_path2file = download(
                website_name = website_name,
                datamining_type = datamining_type,
                payload = payload,
                dt_date_from = dt_date_from,
                dt_date_to = dt_tmp_date_to,
                status_waiting_seconds = status_waiting_seconds,
                date_format = date_format,
            )
            l_path2file.append(output_path2file)

            # last iteration, we queried up to the requested date
            if dt_tmp_date_to == dt_date_to:
                break

            # add one_day_slice to avoid querying the same day twice
            dt_date_from += n_days_slice + one_day_slice

        return l_path2file

    def download_edw(
        self,
        query : str,
        status_waiting_seconds=5,
        ip : str = None,
        output_path2file = None,
        override_file = False,
        jobrun_id = None,
    ) -> str :
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
            If not set, the file will be created in the current
                working directory with a default name

        override_file : bool, optional
            If set to True, will override output_path2file (if exists)
                with the new datamining content
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
            r'{\W+?(\d+)\W+?(\d+)\W+?}', # { 1602958590 1602951590 }
            query
        )
        if not epoch_from_to_findall:
            raise ValueError(f"Could not read epoch_from and epoch_to from query=\n{query}")

        readers_findall = re.findall(r'\w+:\w+@[\w_-]+', query) # ea:pageview@demo-fr
        if not readers_findall:
            raise ValueError(f"Could not read READER from query=\n{query}")

        # for valid filename
        readers = list(
            map(
                lambda s: s.replace(':', '_').replace('@', '_'),
                readers_findall
            )
        )

        if output_path2file:
            if not isinstance(output_path2file, str):
                raise TypeError("output_path2file should be a str type")

        else:
            output_path2file = '_'.join([
                "dw",
                self.__gridpool_name,
                "_".join( epoch_from_to_findall[0]),
                "_".join(readers),
            ])+".cvs.gzip"

            if _is_skippable_request(
                output_path2file=output_path2file,
                override_file=override_file,
                logger=logger
            ):
                return output_path2file

        if ip:
            if not isinstance(ip, str):
                raise ValueError("ip should be a str type")

        else:
            logger.warning("No ip provided\
                \n Fetching external ip from https://api.ipify.org\
                \nif using a vpn, please provide the vpn ip\
            ")
            ip = requests.get(url="https://api.ipify.org").text

        edw_token_url = f"{self.__api_v2}/er/account/get_dw_session_token.json"
        payload = { 'ip' : ip }

        edw_token_json = _request_to_json(
            request_type="get",
            url=edw_token_url,
            headers=self.__http_headers,
            params=payload,
            logger=logger
        )

        edw_token = edw_token_json["data"]["rows"][0][0]
        edw_http_headers =  {
            "Authorization" : "Bearer "+edw_token,
            "Content-Type" : "application/json"
        }

        # no jobrun_id provided, we send the query to get one
        if not jobrun_id:
            search_url =  f"{self.__base_url}:981/edw/jobs"

            edw_json_params = {
                "kind" : "edw#request",
                "query" : query
            }

            search_json = _request_to_json(
                request_type="post",
                url=search_url,
                json_data=edw_json_params,
                headers=edw_http_headers,
                logger=logger
            )

            jobrun_id = search_json['data'][0]

        status_url =  f"{self.__base_url}:981/edw/jobs/{jobrun_id}"

        if not isinstance(status_waiting_seconds, int) or status_waiting_seconds < 5:
            status_waiting_seconds = 5

        ready = False

        while not ready:
            logger.info(f"Waiting for jobrun_id={jobrun_id} to complete")
            time.sleep(status_waiting_seconds)
            status_json = _request_to_json(
                request_type="get",
                url=status_url,
                headers=edw_http_headers,
                logger=logger
            )

            if status_json["status"] == "Done":
                ready = True
                download_url = status_json["data"][1]


        req = urllib.request.Request(
            url=download_url,
            headers = self.__http_headers,
        )


        if req.has_header('Content-Length'):
            logger.info(f"Content-Length={int(req.get_header('Content-Length'))/(1024*1024):.2f} MBs")

        # Stream tmp json file and build csv file
        with gzip.open(
            filename=output_path2file,
            mode="wt"
        ) as csvfile:

            csvwriter = csv.writer(
                csvfile,
                delimiter=';'
            )

            columns = []
            with urllib.request.urlopen(req) as f:
                objects = ijson.items(f, "headers.schema.item")
                headers = (header for header in objects)
                for header in headers:
                    columns.append(header[1])
                csvwriter.writerow(columns)

            with urllib.request.urlopen(req) as f:
                objects = ijson.items(f, "rows.item")
                rows = (row for row in objects)
                for row in rows:
                    csvwriter.writerow(row)

        # Try to kill job, non blocking error
        with urllib.request.urlopen(req) as f:
            objects = ijson.items(f, "headers.uuid.item")
            uuids = (uuid for uuid in objects)
            for uuid in uuids:
                cancel_url = f"{search_url}/{uuid}/cancel"
                try:
                    requests.post(
                        url=cancel_url,
                        headers=edw_http_headers
                    )
                except Exception as e:
                    logger.warning(f"Could not kill the process uuid={uuid}\n\
                                    exception:{e}")

        logger.info(f"Output csv path2file={output_path2file}")

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
        if not isinstance(website_name, str):
            raise TypeError("website_name should be a string")

        view_url = f"{self.__api_v2}/ea/{website_name}/db/view/get_all_name.json"
        view_json = _request_to_json(
            request_type="get",
            url=view_url,
            headers=self.__http_headers,
            logger=logger
        )

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
        """ Fetch website properties

        Parameters
        ----------
        website_name: str, obligatory
            Your targeted website_name in Eulerian Technologies platform

        Returns
        -------
        dict
            A dict as { "website_prop" : "website_prop_value" }
        """
        if not isinstance(website_name, str):
            raise TypeError("website_name should be a string")

        website_url = f"{self.__api_v2}/ea/{website_name}/db/website/get_me.json"
        website_json =  _request_to_json(
            request_type="get",
            url=website_url,
            headers=self.__http_headers,
            logger=logger
        )

        return  {
             website_json["data"]["fields"][i]["name"] : website_json["data"]["rows"][0][i]
                for i in range(len(website_json["data"]["fields"]))
        }
