import os
import json
import subprocess
from bs4 import BeautifulSoup
from charset_normalizer import detect


def import_json_file(file_path):
    """

    Import a json file as a dictionary.

    Args:
        - file_path[str]: file path to the json file

    Returns:
        - data[dict]: dictionary of the json file
    """
    with open(file_path, "r") as file:
        data = json.load(file)
    return data


def export_json_file(file_path, data):
    """

    Export a dictionary to a json file.

    Args:
        - file_path[str]: file path to the json file
        - data[dict]: dictionary of the json file
    Returns:
        - None
    """

    with open(file_path, "w") as file:
        json.dump(data, file)


def mkdir(path_dir):
    """
    Create a directory if it does not exist.

    Args:
        - path_dir[str]: file path to the directory

    Returns:
        - None
    """
    command = f"mkdir '{path_dir}'"
    print(command)
    subprocess.run(command, shell=True, capture_output=True, text=True)


def save_soup_to_html(soup, file_path, encoding="ascii"):
    """

    Saves a BeautifulSoup object as a prettified HTML file.

    Args:
        - soup (BeautifulSoup): The BeautifulSoup object to save.
        - file_path (str): The file path where the HTML should be saved.

    Returns:
        - None
    """

    # Save the raw HTML from the soup object
    with open(file_path, "wb") as file:
        file.write(soup.encode(encoding=encoding))


def find_files_with_regex(directory, regex):
    """
    Finds files in a given directory that match a specified regular expression.

    Args:
        directory (str): The directory in which to search for files.
        regex (str): The regular expression pattern to match file paths against.

    Returns:
        dict: A dictionary where the keys are the file names and the values are the full paths to the files.

    """

    # Run the find command with regex
    result = subprocess.run(
        ["find", directory, "-regex", regex], capture_output=True, text=True, check=True
    )

    # Split the output into lines
    files = result.stdout.strip().split("\n")

    # Create the dictionary with file names as keys and full paths as values
    file_dict = {os.path.basename(file): file for file in files if file}

    return file_dict
