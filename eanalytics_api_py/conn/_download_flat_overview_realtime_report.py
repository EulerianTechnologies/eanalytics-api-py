"""This module allows to download flat realtime report data
from the Eulerian Technologies API"""

import copy

import pandas as pd

from eanalytics_api_py.internal import _request


def download_flat_overview_realtime_report(
        self,
        date_from: str,
        date_to: str,
        website_name: str,
        report_name: list,
        kpi: list,
        channel: list = None,
        view_id: int = 0,
        mdevicetype_id: list = None,
        ordertype_id: list = None,
        ordertypecustom_id: list = None,
        profile_id: list = None,
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

    kpi: list, mandatory
        List of kpis to request

    channel: list, mandatory
        List of channels (ADVERTISING...)

    view_id: int, optional
        Between 0 and 9

    mdevicetype_id: list, optional
        List of mdevicetype_id to filter on

    ordertype_id: list, optional
        List of ordertype_id to filter on

    ordertypecustom_id: list, optional
        List of ordertypecustom_id to filter on

    profile_id: list, optional
        List of profile_id to filter on

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

    if not isinstance(kpi, list):
        raise TypeError("kpi should be a list dtype")

    payload = {
        'date-from': date_from,
        'date-to': date_to,
    }
    if mdevicetype_id:
        if not isinstance(mdevicetype_id, list):
            raise TypeError("mdevicetype_id should be a list")

        mdevicetype_map = self.get_mdevicetype_id_name_map(website_name)
        for _id in mdevicetype_id:
            if _id not in mdevicetype_map:
                raise ValueError(f"mdevicetype_id={_id} not allowed. Allowed={', '.join(mdevicetype_map.keys())}")
        payload["ea-subk"] = ",".join(mdevicetype_id)

    if ordertype_id:
        if not isinstance(ordertype_id, list):
            raise TypeError("ordertype_id should be a list")
        ordertype_map = self.get_ordertype_id_name_map(website_name)
        for _id in ordertype_id:
            if _id not in ordertype_map:
                raise ValueError(f"ordertype_id={_id} not allowed. Allowed={', '.join(ordertype_map.keys())}")
        payload["shoppingcart-k1"] = ",".join(ordertype_id)

    if ordertypecustom_id:
        if not isinstance(ordertypecustom_id, list):
            raise TypeError("ordertypecustom_id should be a list")
        ordertypecustom_map = self.get_ordertypecustom_id_name_map(website_name)
        for _id in ordertypecustom_id:
            if _id not in ordertypecustom_map:
                raise ValueError(
                    f"ordertypecustom_id={_id} not allowed. Allowed={', '.join(ordertypecustom_map.keys())}")
        payload["shoppingcart-k1-custom"] = ",".join(ordertypecustom_id)

    if profile_id:
        if not isinstance(profile_id, list):
            raise TypeError("profile_id should be a list")
        profile_map = self.get_profile_id_name_map(website_name)
        for _id in profile_id:
            if _id not in profile_map:
                raise ValueError(f"profile_id={_id} not allowed. Allowed={', '.join(profile_map.keys())}")
        payload["visitpprofil-k1"] = ",".join(profile_id)

    view_id = str(view_id)
    view_map = self.get_view_id_name_map(website_name)
    if view_id not in view_map:
        raise ValueError(f"view_id={view_id} not found. Allowed: {', '.join(view_map.keys())}")

    payload["view-id"] = view_id

    d_website = self.get_website_by_name(website_name)
    url = f"{self._api_v2}/ea/{website_name}/report/realtime/{report_name}.json"
    path_module = __import__(
        name="eanalytics_api_py.internal.realtime_overview.path._" + report_name,
        fromlist=report_name)

    l_df = []
    d_path = copy.deepcopy(path_module.d_path)  # because we override values we want a clean copy

    if not channel:
        channel = list(d_path.keys())
    for _channel in channel:
        l_path = d_path[_channel]["path"]
        l_path[0] = l_path[0] % int(d_website["website_id"])

        l_dim = d_path[_channel]["dim"]
        if not isinstance(l_dim, list):
            raise TypeError(f"l_dim={l_dim} should be a list dtype")

        payload['path'] = ".".join(l_path)
        payload['ea-columns'] = ",".join([*l_dim, *kpi])

        _json = _request._to_json(
            url=url,
            request_type="get",
            params=payload,
            headers=self._http_headers,
            print_log=True)
        
        sub_df = pd.DataFrame(
            data=_json["data"]["rows"],
            columns=[d_field["name"] for d_field in _json["data"]["fields"]])

        if "add_dim_value_map" in d_path[_channel]:
            for _dim, _value in d_path[_channel]["add_dim_value_map"].items():
                sub_df[_dim] = _value

        if "rename_dim_map" in d_path[_channel]:
            sub_df.rename(
                columns=d_path[_channel]["rename_dim_map"],
                inplace=True)

        l_df.append(sub_df)

    df = pd.concat(
        l_df,
        axis=0,
        ignore_index=True)

    # override name with alias if alias is set
    for name, alias in path_module.override_dim_map.items():
        if all(_ in df.columns for _ in [name, alias]):
            mask = (df[alias].isin([0, '0']))
            df.loc[mask, alias] = df[name]
            df.drop(
                labels=alias,
                axis=1,
                inplace=True)

    df.rename(
        columns=path_module.dim_px_map,
        inplace=True)

    for col_name in df.columns:
        if col_name in path_module.dim_px_map.values():
            df[col_name] = df[col_name].astype("category")
        elif '/' not in col_name:
            df[col_name] = df[col_name].astype("int64")

    return df
