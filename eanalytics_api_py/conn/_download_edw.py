"""This module allows to download the raw data
from the Eulerian Data Warehouse"""

import re
import time
import urllib
import gzip
import csv

import requests
import ijson

from eanalytics_api_py.internal import _request, _log


def download_edw(
        self,
        query: str,
        status_waiting_seconds=5,
        ip: str = None,
        output_path2file=None,
        override_file=False,
        jobrun_id=None,
) -> str:
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
        r'{\W+?(\d+)\W+?(\d+)\W+?}',  # { 1602958590 1602951590 }
        query
    )
    if not epoch_from_to_findall:
        raise ValueError(f"Could not read epoch_from and epoch_to from query=\n{query}")

    readers_findall = re.findall(r'\w+:\w+@[\w_-]+', query)  # ea:pageview@demo-fr
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
            self._gridpool_name,
            "_".join(epoch_from_to_findall[0]),
            "_".join(readers),
        ]) + ".cvs.gzip"

        if _request._is_skippable(
                output_path2file=output_path2file,
                override_file=override_file,
                print_log=self._print_log
        ):
            return output_path2file

    if ip:
        if not isinstance(ip, str):
            raise ValueError("ip should be a str type")

    else:
        self._log("No ip provided\
            \n Fetching external ip from https://api.ipify.org\
            \nif using a vpn, please provide the vpn ip\
        ")
        ip = requests.get(url="https://api.ipify.org").text

    edw_token_url = f"{self._api_v2}/er/account/get_dw_session_token.json"
    payload = {'ip': ip}

    edw_token_json = _request._to_json(
        request_type="get",
        url=edw_token_url,
        headers=self._http_headers,
        params=payload,
        print_log=self._print_log
    )

    edw_token = edw_token_json["data"]["rows"][0][0]
    edw_http_headers = {
        "Authorization": "Bearer " + edw_token,
        "Content-Type": "application/json"
    }

    # no jobrun_id provided, we send the query to get one
    if not jobrun_id:
        search_url = f"{self._base_url}:981/edw/jobs"

        edw_json_params = {
            "kind": "edw#request",
            "query": query
        }

        search_json = _request._to_json(
            request_type="post",
            url=search_url,
            json_data=edw_json_params,
            headers=edw_http_headers,
            print_log=self._print_log
        )
        jobrun_id = search_json['data'][0]

    status_url = f"{self._base_url}:981/edw/jobs/{jobrun_id}"

    if not isinstance(status_waiting_seconds, int) or status_waiting_seconds < 5:
        status_waiting_seconds = 5

    ready = False

    while not ready:
        self._log(f"Waiting for jobrun_id={jobrun_id} to complete")
        time.sleep(status_waiting_seconds)
        status_json = _request._to_json(
            request_type="get",
            url=status_url,
            headers=edw_http_headers,
            print_log=self._print_log
        )

        if status_json["status"] == "Done":
            ready = True
            download_url = status_json["data"][1]

    req = urllib.request.Request(
        url=download_url,
        headers=self._http_headers,
    )
    _stream_to_csv_gzip(
        req=req,
        path2file=output_path2file,
        search_url=search_url,
        edw_http_headers=edw_http_headers,
        print_log=self._print_log
    )

    self._log(f"Output csv path2file={output_path2file}")
    return output_path2file


def _stream_to_csv_gzip(
        req: urllib.request.Request,
        path2file: str,
        search_url: str,
        edw_http_headers: dict,
        print_log : bool = True
) -> None:
    """ Stream the Eulerian Data Warehouse data in csv gzipped file

    Parameters
    ----------
    req : urllib.request.Request, obligatory
        The request object to fetch from

    path2file : str, obligatory
        The path2file to put the data into

    search_url: str, obligatory
        The url to cancel the job

    edw_http_headers: dict, obligatory
        Edw authentification headers

    print_log: bool
        Set to False to hide logs
        Default=True
    """
    if req.has_header('Content-Length'):
        _log._log(
            log=f"Content-Length={int(req.get_header('Content-Length')) / (1024 * 1024):.2f} MBs",
            print_log=print_log)

    with gzip.open(
            filename=path2file,
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
                _log._log(
                    log=f"Could not kill the process uuid={uuid}\n exception:{e}",
                    print_log=print_log)
