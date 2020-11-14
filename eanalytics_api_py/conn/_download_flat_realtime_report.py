"""This module allows to download flat realtime report data
from the Eulerian Technologies API"""

from pprint import pprint

import pandas as pd

from eanalytics_api_py.internal import _request


def download_flat_realtime_report(
        self,
        date_from: str,
        date_to: str,
        website_name: str,
        report_name: list,
        path_dim_map: list,
        kpi: list,
        date_scale: str = '',
        view_id: int = 0,
        filters: dict = None
) -> pd.DataFrame:
    """ Fetch realtime report data into a pandas dataframe

    Parameters
    ----------
    date_from: str, mandatory
        mm/dd/yyyy

    date_to: str, mandatory
        mm/dd/yyyy

    website_name: str, mandatory
        Your targeted website_name in Eulerian Technologies platform

    report_name: str, mandatory

    path_dim_map: list, mandatory
        List of paths to drill down

    kpi: list, mandatory
        List of kpis to request

    date_scale: str, optional
        Split data for a given scale
        Allowed values H, D, W, M

    view_id: int, optional
        Between 0 and 9

    filters: dict, optional
        To filter request result

    Returns
    -------
    pd.DataFrame()
        A pandas dataframe
    """

    if not isinstance(date_from, str):
        raise TypeError("date_from should be a string dtype")

    if not isinstance(date_to, str):
        raise TypeError("date_to should be a string dtype")

    if not isinstance(website_name, str):
        raise TypeError("website_name should be a string dtype")

    if not isinstance(report_name, str):
        raise TypeError("report_name should be a string dtype")

    if not isinstance(path_dim_map, dict):
        raise TypeError("path_dim_map should be a dict dtype")

    if not isinstance(kpi, list):
        raise TypeError("kpi should be a list dtype")

    if not isinstance(date_scale, str):
        raise TypeError("date_scale be a str dtype")

    payload = {
        'date-from': date_from,
        'date-to': date_to,
        'ea-switch-datetorow': 1,  # include the date in each row
        'ea-enable-datefmt': "%s",  # format the date as an epoch timestamp
        'ea-columns': "id," + ",".join(kpi),
    }

    if filters:
        if not isinstance(filters, dict):
            raise TypeError(f"filters={filters} should be a dict dtype")
        filters = self.check_convert_realtime_filter(website_name, filters)
    else:
        filters = {}

    for k in filters.keys():
        if len(filters[k]):
            payload[k] = filters[k]

    l_allowed_scale = ["H", "D", "W", "M"]
    if date_scale and date_scale not in l_allowed_scale:
        raise ValueError(f"date_scale={date_scale} not allowed. Allowed: {', '.join(l_allowed_scale)}")

    view_id = str(view_id)
    view_map = self.get_view_id_name_map(website_name)
    if view_id not in view_map:
        raise ValueError(f"view_id={view_id} not found. Allowed: {', '.join(view_map.keys())}")

    payload["view-id"] = view_id

    d_website = self.get_website_by_name(website_name)
    url = f"{self._api_v2}/ea/{website_name}/report/realtime/{report_name}.json"

    l_df = []
    for path, l_dim in path_dim_map.items():
        if not isinstance(path, str):
            raise ValueError("path in path_dim_map should ba a str dtype")

        if not isinstance(l_dim, list):
            raise ValueError("dim in path_dim_map should ba a list dtype")

        l_path = path.split(".")
        l_path[0] = l_path[0] % int(d_website["website_id"])
        l_all_paths = self._get_all_paths(
            i=1,
            l_path=l_path,
            l_prev_path=[l_path[0]],
            url=url,
            payload=payload)

        sub_df = self._all_paths_to_df(
            url=url,
            l_path=l_all_paths,
            l_dim=l_dim,
            l_kpi=kpi,
            payload=payload,
            date_scale=date_scale)

        l_df.append(sub_df)

    df = pd.concat(
        objs=l_df,
        axis=0,
        ignore_index=True)

    return df


def _get_all_paths(
        self,
        i: int,
        l_path: list,
        l_prev_path: list,
        url: str,
        payload: dict,
):
    l_next_path = []
    for prev_path in l_prev_path:
        if not l_path[i].endswith("[%d]"):
            l_next_path.append(".".join([prev_path, l_path[i]]))

        else:
            payload["path"] = ".".join([prev_path, l_path[i].replace("[%d]", "")])
            _json = _request._to_json(
                url=url,
                request_type="get",
                headers=self._http_headers,
                params=payload,
                print_log=self._print_log
            )
            for _id in _get_ids(_json):
                l_next_path.append(".".join([prev_path, l_path[i] % int(_id)]))

    if i == len(l_path) - 1:
        return l_next_path
    i += 1
    return self._get_all_paths(
        i=i,
        l_path=l_path,
        l_prev_path=l_next_path,
        url=url,
        payload=payload)


def _all_paths_to_df(
        self,
        url: str,
        date_scale: str,
        l_path: [],
        l_dim,
        l_kpi,
        payload: {}
):
    payload["ea-columns"] = "name," + ",".join([*l_dim, *l_kpi])
    if date_scale:
        del(payload["ea-columns"])
        payload["date-scale"] = date_scale
        payload["dd-dt"] = ",".join([*l_dim, *l_kpi])

    l_slice_path = []
    l_df = []
    for i in range(len(l_path)):
        l_slice_path.append(l_path[i])
        if len(l_path) == 1 or (i and (i % 10 == 0 or i == len(l_path) - 1)):
            payload['path'] = ",".join(l_slice_path)
            _json = _request._to_json(
                url=url,
                request_type="get",
                params=payload,
                headers=self._http_headers,
                print_log=self._print_log
            )
            sub_df = pd.DataFrame(
                data=_json["data"]["rows"],
                columns=[d_field["name"] for d_field in _json["data"]["fields"]])

            l_df.append(sub_df)
            l_slice_path = []

    df_concat = pd.concat(
        objs=l_df,
        axis=0,
        ignore_index=True)

    return df_concat


def _get_ids(_json):
    for i, d_header in enumerate(_json["data"]["fields"]):
        if d_header["name"] == "id":
            return (row[i] for row in _json["data"]["rows"])
