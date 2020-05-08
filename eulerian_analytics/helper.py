from pprint import pprint
import inspect
from time import ctime

def has_api_error(_json):
    """ Check for error in the JSON returned by Eulerian Technologies API """
    if 'error' in _json.keys() and _json['error']:
        pprint(_json)
        return 1
    return 0

def log(log):
    """ A simple logging mechanism """
    stack = inspect.stack()
    frame = stack[1]
    caller_func = frame.function
    caller_mod = inspect.getmodule(frame[0])
    log_msg = '%s:%s:%s: %s' %(ctime(), caller_mod.__name__, caller_func, log)
    print(log_msg)