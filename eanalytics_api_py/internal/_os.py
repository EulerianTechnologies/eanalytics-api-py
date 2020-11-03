"""Internal os helper"""

import os
from ._log import _log


def _remove_file(
        path2file: str,
) -> None:
    """ Remove the given path2file

    Parameters
    ----------
    path2file : str, obligatory
        The output full path to file

    Returns
    -------
        A boolean
    """
    if not isinstance(path2file, str):
        raise TypeError(f"path2file={path2file} should be a str type")

    if os.path.isfile(path2file):
        os.remove(path2file)

    return None


def _create_directory(
        output_directory: str
) -> None:
    """ Create directory if it does not exists on the file system

    Parameters
    ----------
    output_directory : str, obligatory
        The output directory to be created on the file system

    """
    if output_directory:
        if not isinstance(output_directory, str):
            raise TypeError("output_directory should be a str type")

        if not os.path.exists(output_directory):
            os.mkdir(output_directory)

    return None
