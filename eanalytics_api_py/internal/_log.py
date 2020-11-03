"""Internal logging module"""

import inspect
import time


def _log(
        log: str,
        print_log: bool = True
) -> None:
    """ A simple logging mechanism
    Parameters
    ----------
    log: str, obligatory
        Log message to be displayed

    print_log: bool, optional
        Whether to print the log message or not
    Returns
    -------
        None, print a log if print_log is True
    """
    if not isinstance(log, str):
        raise TypeError("log should be str dtype")

    if not isinstance(print_log, bool):
        raise TypeError("print_log should be a bool dtype")

    if print_log:
        stack = inspect.stack()
        frame = stack[1]
        caller_func = frame.function
        caller_mod = inspect.getmodule(frame[0])
        log_msg = f"{time.ctime()}:{caller_mod.__name__}:{caller_func}: {log}"
        print(log_msg)

    return None
