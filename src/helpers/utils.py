import logging
from datetime import datetime
import os
import json
from typing import Union

DATETIME_FMT = "%Y-%m-%d %H:%S:%M"
ENCONDING = "utf-8"

DOT_CHAR = "."
JSON_EXTENSION = "json"
APPEND_MODE = ("append", "a")
OVERWRITE_MODE = ("overwrite", "w")

def settup_logger(class_name: str) -> logging.Logger:
    logger = logging.getLogger(class_name)
    handler = logging.StreamHandler()

    logger.setLevel(logging.DEBUG)
    handler.setLevel(logging.DEBUG)

    handler.setFormatter(
        logging.Formatter(fmt="%(asctime)s %(levelname)s %(name)s - %(message)s", datefmt=DATETIME_FMT)
    )

    # Avoid duplicate by disable proparate
    logger.addHandler(handler)
    logger.propagate = False

    return logger


# TODO: Refactor JsonIO class for read/write operation
# DatetimeEncoder override JSOnEndoer default method with ISO format
def parse_iso_datetime(string_time):
    try:
        dtime = datetime.strptime(string_time, DATETIME_FMT).isoformat()
        return dtime
    except ValueError:
        return None
    

def validate_json_file(file_name: str):
    file_name_part = file_name.strip().split(DOT_CHAR)
    extension = file_name_part[-1].lower()

    if extension != JSON_EXTENSION:
        raise ValueError(f"Your file {file_name}is not valid. Require .json file")
    return DOT_CHAR.join([*file_name_part[:-1], extension])


def read_json_file(file_path: str) -> list[dict]:
    print(file_path)
    with open(file=file_path, mode="r", encoding=ENCONDING) as file:
        data = json.load(file)
    return data


def write_json_file(data: Union[dict, list[dict]], file_path: str):
    with open(file=file_path, mode="w", encoding=ENCONDING) as file:
        json.dump(data, file)

    
def export_dict_to_json_file(data: list[dict], folder_path, file_name, write_mode=OVERWRITE_MODE[0], create_folder=False):
    folder_path = folder_path.strip().rstrip("/")
    file_name = validate_json_file(file_name)
    full_path = f"{folder_path}/{file_name}"
    write_mode = write_mode.lower()

    if write_mode.lower() not in [OVERWRITE_MODE[0], APPEND_MODE[0]]:
        raise ValueError(f"Not support {write_mode} mode. Allow write mode: {[OVERWRITE_MODE[0], APPEND_MODE[0]]}")
    
    if not os.path.isdir(folder_path):
        if not create_folder:
            raise Exception(f"{folder_path} is not exists")
        else:
            os.makedirs(folder_path)

    # If mode append, then read old data, combine write with new data
    if write_mode == APPEND_MODE[0] and os.path.isfile(full_path):
        old_data = read_json_file(full_path)
        data.append(old_data)

    write_json_file(data, full_path)
