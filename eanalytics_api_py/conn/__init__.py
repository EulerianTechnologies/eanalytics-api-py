""" This module contains a Conn class to connect and
retrieve data from Eulerian Technologies API
"""

import inspect
import time

from eanalytics_api_py.internal import _request


class Conn:
    """Setup the connexion to Eulerian Technologies API.

    Parameters
    ----------
    gridpool_name: str, obligatory
        Your assigned grid in Eulerian Technologies platform

    datacenter: str, obligatory
        Your assigned datacenter (com for Europe, ca for Canada) in Eulerian Technologies platform

    api_key: str, obligatory
        Your Eulerian Technologies user account API key

    print_log: str, optional
        Set to False to hide log message
        Default: True

    Returns
    -------
        Class is instantiated
    """

    def __init__(
            self,
            gridpool_name: str,
            datacenter: str,
            api_key: str,
            print_log: bool = True
    ):
        if not isinstance(print_log, bool):
            raise TypeError("print_log should be a boolean type")

        if not isinstance(gridpool_name, str) or len(gridpool_name) == 0:
            raise TypeError("gridpool_name should be a non-null string type")

        if not isinstance(datacenter, str) or len(datacenter) == 0:
            raise TypeError("datacenter should be a non-null string type")

        if not isinstance(api_key, str) or len(api_key) == 0:
            raise TypeError("api_key should be a non-null string type")

        self._datacenter = datacenter
        self._gridpool_name = gridpool_name
        self._edw_host = f"https://edw.ea.eulerian.{datacenter}"
        self._edw_jobs = f"{self._edw_host}/edw/jobs"
        self._base_url = f"https://{gridpool_name}.api.eulerian.{datacenter}"
        self._api_v2 = f"{self._base_url}/ea/v2"
        self._api_key = api_key
        self._http_headers = {"Authorization": f"Bearer {api_key}"}
        self._print_log = print_log
        self._check_credentials()

    # Import class methods
    from ._download_datamining import download_datamining
    from ._download_edw import download_edw
    from ._download_realtime_report import download_realtime_report
    from ._download_flat_realtime_report import download_flat_realtime_report, _get_all_paths, _all_paths_to_df
    from ._download_flat_overview_realtime_report import download_flat_overview_realtime_report

    def _check_credentials(self) -> None:
        """Check credentials validity
        set the allowed_website_names attributes
        """
        # raise an error if API error or fail to load as json
        allowed_website_names = []
        overview_url = f"{self._api_v2}/er/account/authtree.json"
        authtree_json = _request._to_json(
            request_type="get",
            url=overview_url,
            headers=self._http_headers,
            print_log=self._print_log)

        for k, v in authtree_json["data"].items():
            allowed_website_names.append(v["website_name"])

        self._allowed_website_names = allowed_website_names

    def _is_allowed_website_name(self,
                                 website_name: str) -> bool:

        if not isinstance(website_name, str):
            raise TypeError(f"website_name={website_name} should be a str instance")

        if website_name in self._allowed_website_names:
            return True

        raise PermissionError(f"You're not allowed to access website_name={website_name}\n",
                              f"Allowed website_name: {', '.join(self._allowed_website_names)}")

    def _log(
            self,
            log: str,
    ) -> None:
        """ A simple logging mechanism
        Parameters
        ----------
        log: str, obligatory
            Log message to be displayed

        Returns
        -------
            None, print a log if print_log is True
        """
        if not isinstance(log, str):
            raise TypeError("log should be str dtype")

        if self._print_log:
            stack = inspect.stack()
            frame = stack[1]
            caller_func = frame.function
            caller_mod = inspect.getmodule(frame[0])
            log_msg = f"{time.ctime()}:{caller_mod.__name__}:{caller_func}: {log}"
            print(log_msg)

        return None

    def _logrewind(
            self,
            log: str,
    ) -> None:
        """ A simple logging mechanism
        Parameters
        ----------
        log: str, obligatory
            Log message to be displayed

        Returns
        -------
            None, print a log if print_log is True
        """
        if not isinstance(log, str):
            raise TypeError("log should be str dtype")

        if self._print_log:
            stack = inspect.stack()
            frame = stack[1]
            caller_func = frame.function
            caller_mod = inspect.getmodule(frame[0])
            log_msg = f"{time.ctime()}:{caller_mod.__name__}:{caller_func}: {log}"
            print("\33[2K" + log_msg, end="\r")

        return None

    def check_convert_realtime_filter(
            self,
            website_name: str,
            d_filter: dict
    ) -> None:
        """ Check filter configuration
        Parameters
        ----------
        d_filter: dict, obligatory
            Dict of filters to be applied for realtime datasource requests

        website_name: str, obligatory
            Targeted website_name in Eulerian Technologies platform
        Returns
        -------
            None, raise an error if bad configuration
        """
        if not isinstance(d_filter, dict):
            raise TypeError(f"d_filter={d_filter} should be a dict dtype")

        d_ret = {}
        profile_map = self.get_profile_id_name_map(website_name)

        for filter_k, filter_v in d_filter.items():
            if not isinstance(filter_v, list):
                raise TypeError(f"filter_v={filter_v} should be a list dtype")

            if filter_k == "mdevicetype-id" and len(d_filter[filter_k]):
                mdevicetype_map = self.get_mdevicetype_id_name_map(website_name)
                for mdevicetype_id in d_filter[filter_k]:
                    if mdevicetype_id not in mdevicetype_id:
                        raise ValueError(f"{filter_k}={mdevicetype_id}" not in {','.join(mdevicetype_map.keys())})
                d_ret["ea-subk"] = ",".join(d_filter[filter_k])

            if filter_k == "ordertype-id" and len(d_filter[filter_k]):
                ordertype_map = self.get_ordertype_id_name_map(website_name)
                for ordertype_id in d_filter[filter_k]:
                    if ordertype_id not in ordertype_id:
                        raise ValueError(f"{filter_k}={ordertype_id}" not in {','.join(ordertype_map.keys())})
                d_ret["shoppingcart-k1"] = ",".join(d_filter[filter_k])

            if filter_k == "estimatetype-id" and len(d_filter[filter_k]):
                estimatetype_map = self.get_estimatetype_id_name_map(website_name)
                for estimatetype_id in d_filter[filter_k]:
                    if estimatetype_id not in estimatetype_id:
                        raise ValueError(f"{filter_k}={estimatetype_id}" not in {','.join(estimatetype_map.keys())})
                d_ret["estimate-k1"] = ",".join(d_filter[filter_k])

            if filter_k == "ordertypecustom-id" and len(d_filter[filter_k]):
                ordertypecustom_map = self.get_ordertypecustom_id_name_map(website_name)
                for ordertypecustom_id in d_filter[filter_k]:
                    if ordertypecustom_id not in ordertypecustom_id:
                        raise ValueError(
                            f"{filter_k}={ordertypecustom_id}" not in {','.join(ordertypecustom_map.keys())})
                d_ret["shoppingcart-k1-custom"] = ",".join(d_filter[filter_k])

            if filter_k == "orderpayment-id" and len(d_filter[filter_k]):
                orderpayment_map = self.get_orderpayment_id_name_map(website_name)
                for orderpayment_id in d_filter[filter_k]:
                    if orderpayment_id not in orderpayment_id:
                        raise ValueError(f"{filter_k}={orderpayment_id}" not in {','.join(orderpayment_map.keys())})
                d_ret["shoppingcart-k2"] = ",".join(d_filter[filter_k])

            if filter_k in ["profilevisit-id"] and len(d_filter[filter_k]):
                for profile_id in d_filter[filter_k]:
                    if profile_id not in profile_map:
                        raise ValueError(
                            f"{filter_k}={profile_id} not allowed. Allowed={', '.join(profile_map.keys())}")
                d_ret["visitpprofil-k1"] = ",".join(d_filter[filter_k])

            if filter_k in ['profilechange-session-id'] and len(d_filter[filter_k]):
                for profile_id in d_filter[filter_k]:
                    if profile_id not in profile_map:
                        raise ValueError(
                            f"{filter_k}={profile_id} not allowed. Allowed={', '.join(profile_map.keys())}")
                d_ret["profilechange-k1"] = ",".join(d_filter[filter_k])

            if filter_k in ['profilechange-global-id'] and len(d_filter[filter_k]):
                for profile_id in d_filter[filter_k]:
                    if profile_id not in profile_map:
                        raise ValueError(
                            f"{filter_k}={profile_id} not allowed. Allowed={', '.join(profile_map.keys())}")
                d_ret["profilechange-k2"] = ",".join(d_filter[filter_k])

        return d_ret

    def get_view_id_name_map(
            self,
            website_name: str
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

        view_url = f"{self._api_v2}/ea/{website_name}/db/view/get_all_name.json"
        view_json = _request._to_json(
            request_type="get",
            url=view_url,
            headers=self._http_headers,
            print_log=self._print_log
        )

        view_id_idx = view_json["data"]["fields"].index({"name": "view_id"})
        view_name_idx = view_json["data"]["fields"].index({"name": "view_name"})
        views = {view[view_id_idx]: view[view_name_idx] for view in view_json["data"]["rows"]}

        if "0" not in views:
            views["0"] = "last channel"

        return views

    def get_website_by_name(
            self,
            website_name: str
    ) -> dict:
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

        website_url = f"{self._api_v2}/ea/{website_name}/db/website/get_me.json"
        website_json = _request._to_json(
            request_type="get",
            url=website_url,
            params={"output-as-kv": 1},
            headers=self._http_headers,
            print_log=self._print_log
        )

        d_website = website_json["data"]["rows"][0]
        if not isinstance(d_website, dict):
            raise TypeError(f"d_website={d_website} should be a dict dtype")
        return d_website

    def get_mdevicetype_id_name_map(
            self,
            website_name: str,
    ) -> dict:
        """ Fetch id and name properties of mdevicetype class

        Parameters
        ----------
        website_name: str, obligatory
            Your targeted website_name in Eulerian Technologies platform

        Returns
        -------
        dict
            A dict as { "mdevicetype_id" : "mdevicetype_name" }
        """
        if not isinstance(website_name, str):
            raise TypeError("website_name should be a string")

        mdevicetype_url = f"{self._api_v2}/ea/{website_name}/db/mdevicetype/getall.json"
        _json = _request._to_json(
            request_type="get",
            url=mdevicetype_url,
            params={"output-as-kv": 1},
            headers=self._http_headers,
            print_log=self._print_log
        )

        return {
            _json["data"]["rows"][i]["mdevicetype_id"]: _json["data"]["rows"][i]["mdevicetype_name"]
            for i in range(len(_json["data"]["rows"]))
        }

    def get_ordertype_id_name_map(
            self,
            website_name: str,
    ) -> dict:
        """ Fetch id and name properties of the ordertype class

        Parameters
        ----------
        website_name: str, obligatory
            Your targeted website_name in Eulerian Technologies platform

        Returns
        -------
        dict
            A dict as { "ordertype_id" : "ordertype_name" }
        """
        if not isinstance(website_name, str):
            raise TypeError("website_name should be a string")

        ordertype_url = f"{self._api_v2}/ea/{website_name}/db/ordertype/searchvisible.json"
        _json = _request._to_json(
            request_type="get",
            url=ordertype_url,
            params={"limit": 500, "output-as-kv": 1},
            headers=self._http_headers,
            print_log=self._print_log
        )

        return {
            _json["data"]["rows"][i]["ordertype_id"]: _json["data"]["rows"][i]["ordertype_key"]
            for i in range(len(_json["data"]["rows"]))
        }

    def get_estimatetype_id_name_map(
            self,
            website_name: str,
    ) -> dict:
        """ Fetch id and name properties of the estimatetype class

        Parameters
        ----------
        website_name: str, obligatory
            Your targeted website_name in Eulerian Technologies platform

        Returns
        -------
        dict
            A dict as { "estimatetype_id" : "estimatetype_name" }
        """
        if not isinstance(website_name, str):
            raise TypeError("website_name should be a string")

        estimatetype_url = f"{self._api_v2}/ea/{website_name}/db/estimatetype/searchvisible.json"
        _json = _request._to_json(
            request_type="get",
            url=estimatetype_url,
            params={"limit": 500, "output-as-kv": 1},
            headers=self._http_headers,
            print_log=self._print_log
        )

        return {
            _json["data"]["rows"][i]["estimatetype_id"]: _json["data"]["rows"][i]["estimatetype_key"]
            for i in range(len(_json["data"]["rows"]))
        }

    def get_orderpayment_id_name_map(
            self,
            website_name: str,
    ) -> dict:
        """ Fetch id and name properties of the orderpayment class

        Parameters
        ----------
        website_name: str, obligatory
            Your targeted website_name in Eulerian Technologies platform

        Returns
        -------
        dict
            A dict as { "orderpayment_id" : "orderpayment_name" }
        """
        if not isinstance(website_name, str):
            raise TypeError("website_name should be a string")

        orderpayment_url = f"{self._api_v2}/ea/{website_name}/db/orderpayment/searchvisible.json"
        _json = _request._to_json(
            request_type="get",
            url=orderpayment_url,
            params={"limit": 500, "output-as-kv": 1},
            headers=self._http_headers,
            print_log=self._print_log
        )

        return {
            _json["data"]["rows"][i]["orderpayment_id"]: _json["data"]["rows"][i]["orderpayment_key"]
            for i in range(len(_json["data"]["rows"]))
        }

    def get_ordertypecustom_id_name_map(
            self,
            website_name: str,
    ) -> dict:
        """ Fetch id and name properties of the ordertypecustom class

        Parameters
        ----------
        website_name: str, obligatory
            Your targeted website_name in Eulerian Technologies platform

        Returns
        -------
        dict
            A dict as { "ordertypecustom_id" : "ordertypecustom_name" }
        """
        if not isinstance(website_name, str):
            raise TypeError("website_name should be a string")

        ordertypecustom_url = f"{self._api_v2}/ea/{website_name}/db/ordertypecustom/searchvisible.json"
        _json = _request._to_json(
            request_type="get",
            url=ordertypecustom_url,
            params={"limit": 100, "output-as-kv": 1},
            headers=self._http_headers,
            print_log=self._print_log
        )

        return {
            _json["data"]["rows"][i]["ordertypecustom_id"]: _json["data"]["rows"][i]["ordertypecustom_name"]
            for i in range(len(_json["data"]["rows"]))
        }

    def get_profile_id_name_map(
            self,
            website_name: str,
    ) -> dict:
        """ Fetch id and name properties of the profile class

        Parameters
        ----------
        website_name: str, obligatory
            Your targeted website_name in Eulerian Technologies platform

        Returns
        -------
        dict
            A dict as { "profile_id" : "profile_name" }
        """
        if not isinstance(website_name, str):
            raise TypeError("website_name should be a string")

        profile_url = f"{self._api_v2}/ea/{website_name}/db/profile/search.json"
        _json = _request._to_json(
            request_type="get",
            url=profile_url,
            params={"limit": 100, "output-as-kv": 1},
            headers=self._http_headers,
            print_log=self._print_log
        )

        return {
            _json["data"]["rows"][i]["profile_id"]: _json["data"]["rows"][i]["profile_name"]
            for i in range(len(_json["data"]["rows"]))
        }
