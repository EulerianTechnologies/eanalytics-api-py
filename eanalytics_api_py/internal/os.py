"""Internal os helper"""

import os

def _remove_file(
    path2file : str,
    logger,
) -> None:
    """ Remove the given path2file

    Parameters
    ----------
    path2file : str, obligatory
        The output full path to file

    logger : the logging.getLogger object

    Returns
    -------
        A boolean
    """
    if not isinstance(path2file, str):
        raise TypeError(f"path2file={path2file} should be a str type")

    if os.path.isfile(path2file):
        logger.info(f"Deleting path2file={path2file}")
        os.remove(path2file)


def _is_skippable_request(
    output_path2file : str,
    override_file : bool,
    logger
) -> bool :
    """ Load data from local file is exist and override_file is True

    Parameters
    ----------
    output_path2file : str, obligatory
        The output full path to file

    override_file : bool, obligatory
        Your assigned datacenter (com for Europe, ca for Canada)

    logger : the logging.getLogger object

    Returns
    -------
        A boolean
    """
    if not isinstance(output_path2file, str):
        raise TypeError("output_path2file should be a string type")

    if os.path.isfile(output_path2file):
        if override_file:
            logger.info(f'Local path2file={output_path2file} will be overridden with new data')
            return 0

        logger.info(f'Output_path2file={output_path2file} already exists, skipping download')
        return 1

    logger.info(f'path2file={output_path2file} not found, downloading new data')
    return 0
