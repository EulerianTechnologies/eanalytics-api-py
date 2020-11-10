""" This module contains a Conn class to connect and
retrieve data from Eulerian Technologies API
"""

import hashlib
import inspect
import time

from ..internal import _request


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
    _cached_creds = set()

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

        self._cached_credentials = set()
        self._datacenter = datacenter
        self._gridpool_name = gridpool_name
        self._base_url = f"https://{gridpool_name}.api.eulerian.{datacenter}"
        self._api_v2 = f"{self._base_url}/ea/v2"
        self._api_key = api_key
        self._http_headers = {"Authorization": f"Bearer {api_key}"}
        self._print_log = print_log

        if self._creds_hex_digest in Conn._cached_creds:
            self._log("Credentials found in cache")

        else:
            self._check_credentials()
            Conn._cached_creds.add(self._creds_hex_digest)
            self._log("New credentials ok")

    # Import class methods
    from ._download_datamining import download_datamining
    from ._download_edw import download_edw
    from ._download_realtime_report import download_realtime_report
    from ._download_flat_realtime_report import download_flat_realtime_report, _get_all_paths, _all_paths_to_df
    from ._download_flat_overview_realtime_report import download_flat_overview_realtime_report

    @property
    def _creds_hex_digest(self) -> str:
        concate_creds_values = ''.join([
            self._datacenter,
            self._gridpool_name,
            self._api_key
        ])
        hash_object = hashlib.md5(concate_creds_values.encode())
        creds_hex_digest = hash_object.hexdigest()
        return creds_hex_digest

    def _check_credentials(self) -> None:
        """Check credentials validity"""
        # raise an error if API error or fail to load as json
        overview_url = f"{self._api_v2}/er/account/authtree.json"
        _request._to_json(
            request_type="get",
            url=overview_url,
            headers=self._http_headers,
            print_log=self._print_log
        )

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
