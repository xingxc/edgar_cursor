import json
import subprocess
from bs4 import BeautifulSoup


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


def save_soup_to_html(soup, file_path):
    """

    Saves a BeautifulSoup object as a prettified HTML file.

    Args:
        - soup (BeautifulSoup): The BeautifulSoup object to save.
        - file_path (str): The file path where the HTML should be saved.

    Returns:
        - None
    """
    with open(file_path, "w") as file:
        file.write(soup.prettify())
