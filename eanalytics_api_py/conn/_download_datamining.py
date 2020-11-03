""" This module allows to download a datamining
from the Eulerian Technologies API
"""

import urllib
import re
import os
import gzip
import time
from datetime import datetime, timedelta
import csv

import ijson

from eanalytics_api_py.internal import _os, _request


def download_datamining(
        self,
        website_name: str,
        datamining_type: str,
        payload=None,
        status_waiting_seconds=5,
        output_directory='',
        override_file=False,
        n_days_slice=31,
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
    if not isinstance(website_name, str):
        raise TypeError("website_name should be a str type")

    if not isinstance(datamining_type, str):
        raise TypeError("datamining_type should be a str type")

    if not isinstance(payload, dict) or not payload:
        raise TypeError("payload should be a non-empty dict")

    if not isinstance(n_days_slice, int) or n_days_slice < 0:
        raise TypeError("n_days_slice should be a positive integer")

    l_allowed_datamining_types = ["order", "estimate", "isenginerequest", "actionlog", "scart"]

    if datamining_type not in l_allowed_datamining_types:
        raise ValueError(f"datamining_type={datamining_type} not allowed.\n\
                        Use one of the following: {', '.join(l_allowed_datamining_types)}")

    date_from = payload["date-from"] if "date-from" in payload else None
    if not date_from:
        raise ValueError("missing parameter=date-from in payload object")

    date_to = payload['date-to'] if 'date-to' in payload else None
    if not date_to:
        raise ValueError("missing parameter=date-from in payload object")

    date_format = "%m/%d/%Y"
    dt_date_from = datetime.strptime(date_from, date_format)
    dt_date_to = datetime.strptime(date_to, date_format)

    if dt_date_from > dt_date_to:
        raise ValueError("'date-from' cannot occur later than 'date-to'")

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

    website_name = website_name
    datamining_type = datamining_type
    payload = payload
    status_waiting_seconds = status_waiting_seconds
    n_days_slice = n_days_slice
    dt_tmp_date_to = dt_date_to
    n_days_slice = timedelta(days=n_days_slice)
    one_day_slice = timedelta(days=1)
    _os._create_directory(output_directory=output_directory)
    l_path2file = []  # store each file for n_days_slice
    # To avoid overloading the API with huge requests
    # We split the requests into smaller timeranres "n_days_slice"
    # While temporary date to delta is smaller than requested time to delta
    while dt_tmp_date_to <= dt_date_to:
        dt_tmp_date_to = dt_date_from + n_days_slice
        # Cannot request further than dt_date_to
        dt_tmp_date_to = dt_date_to if dt_tmp_date_to >= dt_date_to else dt_tmp_date_to
        date_from = dt_date_from.strftime(date_format)
        date_to = dt_tmp_date_to.strftime(date_format)
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
        ]) + ".csv.gz"
        output_path2file = os.path.join(output_directory, output_filename)

        if not _request._is_skippable(
                output_path2file=output_path2file,
                override_file=override_file,
                print_log=self._print_log
        ):
            payload['date-from'] = date_from
            payload['date-to'] = date_to

            search_url = f"{self._api_v2}/ea/{website_name}/report/{datamining_type}/search.json"

            search_json = _request._to_json(
                request_type="get",
                url=search_url,
                params=payload,
                headers=self._http_headers,
                print_log=self._print_log
            )

            jobrun_id = search_json["jobrun_id"]

            status_url = f"{self._api_v2}/ea/{website_name}/report/{datamining_type}/status.json"
            status_payload = {"jobrun-id": jobrun_id}
            ready = False

            if not isinstance(status_waiting_seconds, int) or status_waiting_seconds < 5:
                status_waiting_seconds = 5

            while not ready:
                self._log(f'Waiting for jobrun_id={jobrun_id} to complete')
                time.sleep(status_waiting_seconds)
                status_json = _request._to_json(
                    request_type="get",
                    url=status_url,
                    params=status_payload,
                    headers=self._http_headers,
                    print_log=self._print_log
                )

                if status_json["jobrun_status"] == "COMPLETED":
                    ready = True

            download_url = f"{self._api_v2}/ea/{website_name}/report/{datamining_type}/download.json"
            download_payload = {'output-as-csv': 0, 'jobrun-id': jobrun_id}

            req = urllib.request.Request(
                url=f"{download_url}?{urllib.parse.urlencode(download_payload)}",
                headers=self._http_headers,
            )
            _stream_req(
                req=req,
                output_path2file=output_path2file)
        l_path2file.append(output_path2file)

        # last iteration, we queried up to the requested date
        if dt_tmp_date_to == dt_date_to:
            break

        # add one_day_slice to avoid querying the same day twice
        dt_date_from += n_days_slice + one_day_slice

    return l_path2file


def _stream_req(
    req: urllib.request.Request,
    output_path2file: str
) -> None:
    """ Stream datamining data in csv gzipped file

    Parameters
    ----------
    req: urllib.request.Request, obligatory
    output_path2file: str, obligatory
    """
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
            objects = ijson.items(f, "data.fields.item")  # .item is for ijson
            headers = (header for header in objects)
            # working on header.name rather than header.header for consitency
            # because the latest is language specific
            for header in headers:
                # this allows to recover the name of the product param name
                # instead of the id
                if header["name"].startswith('productparam_'):
                    match = re.search(
                        pattern=r'\s:\s([\w\W]+?)\s#\s(\d+)$',
                        string=header["header"]
                    )
                    prdp_name = match.group(1)
                    prd_idx = int(match.group(2)) - 1  # start at 0
                    header["name"] = f"productparam_{prdp_name}_{prd_idx}"
                # this allows to recover the name of the cgi param name
                # instead of the id
                if header["name"].startswith('cgiparam_'):
                    match = re.search(
                        pattern=r'^[\w\W]+?\s:\s(.*)$',
                        string=header["header"],
                    )
                    cgip_name = match.group(1)
                    header["name"] = f"cgiparam_{cgip_name}"
                # this allows to recover the name of the CRM param name
                # instead of the id
                if header["name"].startswith('iduserparam_'):
                    match = re.search(
                        pattern=r'^[\w\W]+?\s:\s(.*)$',
                        string=header["header"],
                    )
                    iduserparam_name = match.group(1)
                    header["name"] = f"iduserparam_{iduserparam_name}"
                # this allows to recover the name of the audience name
                # instead of the id
                if header["name"].startswith('cluster_'):
                    match = re.search(
                        pattern=r'^[\w\W]+?\s:\s(.*)$',
                        string=header["header"],
                    )
                    cluster_name = match.group(1)
                    header["name"] = f"cluster_{cluster_name}"

                columns.append(header["name"])
            csvwriter.writerow(columns)

        with urllib.request.urlopen(req) as f:
            objects = ijson.items(f, "data.rows.item")  # .item is for ijson
            rows = (row for row in objects)
            for row in rows:
                csvwriter.writerow(row)
