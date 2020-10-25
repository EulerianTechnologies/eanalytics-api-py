"""Request helper module"""

from pprint import pformat
import urllib

import requests

def _request_to_json(
    request_type : str,
    url : str,
    logger,
    headers : dict = None,
    params : dict = None,
    json_data : dict = None,
) -> dict :
    """ Make HTTP request and check for error

    Parameters
    ----------
    request_type : str, obligatory
        The type of request : get/post supported at the moment

    headers : dict, optional
        The dict to use as the request header

    params : dict, optional
        The dict to use as the request params (requests.get)

    json_data : dict, optional
        The dict to use as the request json params (requests.post)

    logger: the logging.getLogger object

    Returns
    -------
        Request response loaded as JSON
    """
    if not isinstance(request_type, str):
        raise TypeError("request_type should be a string dtype")

    if not isinstance(url, str):
        raise TypeError("url should be a string dtype")

    if headers:
        if not isinstance(headers, dict):
            raise TypeError("headers should be a dict dtype")

    if params:
        if not isinstance(params, dict):
            raise TypeError("params should be a dict dtype")

    if json_data:
        if not isinstance(json_data, dict):
            raise TypeError("json_data should be a dict dtype")

    request_map = {
        'get' : requests.get,
        'post' : requests.post
    }

    allowed_requests_type = ["get", "post"]

    if request_type not in allowed_requests_type:
        raise ValueError(f"request_type is not in {', '.join(allowed_requests_type)}")

    params = urllib.parse.urlencode(params) if params else ''
    logger.debug(f"url={url}?{params}")
    logger.debug(f"headers=\n{pformat(headers)}")
    logger.debug(f"json_data=\n{pformat(json_data)}")

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

    # if request cannot be converted into JSON
    try:
        r_json = r.json()

    # JSONDecodeError is a subclass of ValueError
    except ValueError as e:
        logger.critical(f"Could not convert'{r.text}' as json")
        raise e

    else:
        # potential errors from Eulerian Technologies API
        if "error" in r_json.keys() and r_json["error"] \
        or "status" in r_json.keys() and r_json['status'].lower() == "failed":

            logger.critical(f"JSON response from the API\n{pformat(r_json)}")
            raise SystemError(f"Error[{r.status_code}] from Eulerian Technologies API")

    return r_json
