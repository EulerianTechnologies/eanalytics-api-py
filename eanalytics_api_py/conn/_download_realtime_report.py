"""This module allows to download realtime report data
from the Eulerian Technologies API"""

import pandas as pd

from eanalytics_api_py.internal import _request


def download_realtime_report(
        self,
        website_name: str,
        report_name: list,
        payload: dict,
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
        raise TypeError("report_name should be a str type")

    if not isinstance(payload, dict):
        raise TypeError("payload should be a dict type")

    if not payload:
        raise ValueError("payload should not be empty")

    report_url = f"{self._api_v2}/ea/{website_name}/report/realtime/{report_name}.json"
    payload['ea-switch-datetorow'] = 1  # include the date in each row
    payload['ea-enable-datefmt'] = "%s"  # format the date as an epoch timestamp

    report_json = _request._to_json(
        request_type="get",
        url=report_url,
        params=payload,
        headers=self._http_headers,
        print_log=self._print_log
    )

    fields = [field['name'] for field in report_json['data']['fields']]
    rows = report_json['data']['rows']

    df = pd.DataFrame(
        columns=fields,
        data=rows,
    )

    df["website"] = website_name  # useful when querying multiple websites will be implemented
    df["date"] = pd.to_datetime(df["date"], unit='s')

    return df
