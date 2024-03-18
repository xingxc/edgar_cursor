import json


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
