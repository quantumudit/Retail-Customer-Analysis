"""
This module provides utility functions for handling files and directories.
It includes functions for reading YAML files, CSV files, creating directories
and writing to CSV files. The functions are designed to handle exceptions and
log relevant information for debugging purposes.
"""
import zipfile
from os import listdir, makedirs
from os.path import normpath

import yaml
from box import Box

from src.exception import CustomException
from src.logger import logger


def read_yaml(yaml_path: str) -> Box:
    """
    This function reads a YAML file from the provided path and returns
    its content as a Box object.

    Args:
        yaml_path (str): The path to the YAML file to be read.

    Raises:
        CustomException: If there is any error while reading the file or
        loading its content, a CustomException is raised with the original
        exception as its argument.

    Returns:
        Box: The content of the YAML file, loaded into a Box object for
        easy access and manipulation.
    """
    try:
        yaml_path = normpath(yaml_path)
        with open(yaml_path, "r", encoding="utf-8") as yf:
            content = Box(yaml.safe_load(yf))
            logger.info("yaml file: %s loaded successfully", yaml_path)
            return content
    except Exception as e:
        logger.error(CustomException(e))
        raise CustomException(e) from e


def create_directories(dir_paths: list, verbose=True) -> None:
    """
    This function creates directories at the specified paths.

    Args:
        dir_paths (list): A list of directory paths where directories need
        to be created.
        verbose (bool, optional): If set to True, the function will log
        a message for each directory it creates. Defaults to True.
    """
    for path in dir_paths:
        makedirs(normpath(path), exist_ok=True)
        if verbose:
            logger.info("created directory at: %s", path)


def unzip_file(zipfile_path: str, unzip_dir: str) -> str:
    """
    Unzips a file to a specified directory.

    Args:
        zipfile_path (str): The path to the zip file.
        unzip_dir (str): The directory where the files will be extracted.

    Returns:
        str: A list of the names of the extracted files.
    """
    zipfile_path = normpath(zipfile_path)
    unzip_dir = normpath(unzip_dir)
    try:
        with zipfile.ZipFile(zipfile_path, "r") as zf:
            zf.extractall(path=unzip_dir)
        unzipped_files = listdir(unzip_dir)
        return unzipped_files
    except Exception as e:
        logger.error(CustomException(e))
        raise CustomException(e) from e
