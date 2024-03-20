import json
import subprocess


def import_json_file(file_path):
    """
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
    Args:
        - path_dir[str]: file path to the directory

    Returns:
        - None
    """
    command = f"mkdir '{path_dir}'"
    print(command)
    subprocess.run(command, shell=True, capture_output=True, text=True)

