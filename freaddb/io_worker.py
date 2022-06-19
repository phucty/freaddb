import os
import ujson


def create_dir(file_dir):
    folder_dir = os.path.dirname(file_dir)
    if not os.path.exists(folder_dir):
        os.makedirs(folder_dir)


def save_json_file(file_name: str, json_obj: dict):
    with open(file_name, "w") as f:
        ujson.dump(json_obj, f)


def read_json_file(file_name: str):
    with open(file_name, "r") as f:
        return ujson.load(f)