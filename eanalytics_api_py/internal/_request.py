"""Request helper module"""

from pprint import pprint
import urllib
import os

import requests

from ._log import _log


def _to_json(
        request_type: str,
        url: str,
        headers: dict = None,
        params: dict = None,
        json_data: dict = None,
        print_log: bool = False
) -> dict:
    """ Make HTTP request and check for error

    Parameters
    ----------
    request_type: str, obligatory
        The type of request : get/post supported at the moment

    url: str, obligatory
        The url to request

    headers: dict, optional
        The dict to use as the request header

    params: dict, optional
        The dict to use as the request params (requests.get)

    json_data: dict, optional
        The dict to use as the request json params (requests.post)

    print_log: bool, optional
        Default: True
    Returns
    -------
        Request response loaded as JSON
    """
    if not isinstance(request_type, str):
        raise TypeError("request_type should be a string dtype")

    if not isinstance(url, str):
        raise TypeError("url should be a string dtype")

    if headers and not isinstance(headers, dict):
        raise TypeError("headers should be a dict dtype")

    if params and not isinstance(params, dict):
        raise TypeError("params should be a dict dtype")

    if json_data and not isinstance(json_data, dict):
        raise TypeError("json_data should be a dict dtype")

    request_map = {
        "get": requests.get,
        "post": requests.post
    }

    api_key = headers["Authorization"].split(" ")[1]
    log_url = url.replace("/ea/v2/", f"/ea/v2/{api_key}/")

    params = urllib.parse.urlencode(params, safe='/') if params else ''
    _log(
        log=f"url={log_url}?{params}",
        print_log=print_log
    )

    if request_type == "get":
        r = request_map["get"](
            url=url,
            headers=headers,
            params=params
        )

    elif request_type == "post":
        r = request_map["post"](
            url=url,
            headers=headers,
            json=json_data
        )

    else:
        allowed_requests_type = ["get", "post"]
        raise ValueError(f"request_type is not in {', '.join(allowed_requests_type)}")

    # if request cannot be converted into JSON
    try:
        r_json = r.json()

    # JSONDecodeError is a subclass of ValueError
    except ValueError as e:
        print(f"Could not convert'{r.text}' as json")
        raise e

    else:
        # potential errors from Eulerian Technologies API
        if "error" in r_json.keys() and r_json["error"] \
                or "status" in r_json.keys() and r_json['status'].lower() == "failed":
            print("JSON response from Eulerian Technologies API")
            pprint(r_json)
            raise SystemError(f"Error[{r.status_code}] from Eulerian Technologies API")

    return r_json


def _is_skippable(
        output_path2file: str,
        override_file: bool,
        print_log: bool = True
) -> bool:
    """ Load data from local file is exist and override_file is True

    Parameters
    ----------
    output_path2file : str, obligatory
        The output full path to file

    override_file : bool, obligatory
        Your assigned datacenter (com for Europe, ca for Canada)

    print_log: bool, optional
        Set to False to not display logs
        Default: True
    Returns
    -------
        True if we can fetch data directly from the file
    """
    if not isinstance(output_path2file, str):
        raise TypeError("output_path2file should be a string type")

    if not isinstance(override_file, bool):
        raise TypeError("override_file should be a string type")

    if os.path.isfile(output_path2file):
        if override_file:
            _log(
                log=f"Local file={output_path2file} will be overriden with new data",
                print_log=print_log)
            return False
        _log(
            log=f"Fetching data from local file={output_path2file}",
            print_log=print_log)
        return True

    _log(
        log=f"Local file={output_path2file} not found, downloading the data",
        print_log=print_log)
    return False
