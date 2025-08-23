import logging
from datetime import datetime
import os
import json
from typing import Union
from uuid import uuid1
import subprocess

DATETIME_FMT = "%Y-%m-%d %H:%S:%M"
ENCONDING = "utf-8"

DOT_CHAR = "."
JSON_EXTENSION = "json"
PARQUET_EXTENSION = "parquet"
CSV_EXTENSION = "csv"
SQL_EXTENSION = "sql"
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


# TODO: Refactor JsonIO Class for read/write operation
# DatetimeEncoder override JSonEndoer default method with ISO format
def parse_iso_datetime(string_time):
    try:
        dtime = datetime.strptime(string_time, DATETIME_FMT).isoformat()
        return dtime
    except ValueError:
        return None
    

def check_exists_file(file_path: str):
    # Only rstrip to handle error whether user using relative path
    file_path = file_path.rstrip()
    if os.path.isfile(file_path):
        return True
    else:
        return False
    
def check_exists_folder(folder_path: str):
    folder_path = folder_path.rstrip()
    if os.path.isdir(folder_path):
        return True
    else:
        return False
    
def create_if_not_exist_folder(folder_path: str):
    if not check_exists_folder(folder_path):
        os.makedirs(folder_path)


def create_temp_folder(folder_name):
    folder_path = f"/tmp/{folder_name}"
    create_if_not_exist_folder(folder_path)
    return folder_path


def remove_folder(folder_path):
    print(folder_path)
    try:
        if check_exists_folder(folder_path):
            subprocess.run(["rm", "-rf", folder_path], check=True)
    except subprocess.CalledProcessError as e:
        raise Exception(f"Error remove folder: {e}")
    

def upload_folder(folder_source, folder_target):
    print(folder_source, folder_target)
    try:
        if check_exists_folder(folder_source):
            subprocess.run(["cp", "-r", f"{folder_source}/", folder_target], check=True)
    except subprocess.CalledProcessError as e:
        raise Exception(f"Error upload folder {folder_source} to {folder_target}: {e}")


def identify_file_type(file_name: str):
    """This function validate if file name is one of CSV, PARQUET, JSON file"""
    file_name_part = file_name.strip().split(DOT_CHAR)
    extension = file_name_part[-1].lower()
    cleaned_file_name = DOT_CHAR.join([*file_name_part[:-1], extension])

    if extension == JSON_EXTENSION:
        return JSON_EXTENSION, cleaned_file_name
    elif extension == CSV_EXTENSION:
        return CSV_EXTENSION, cleaned_file_name
    elif extension == PARQUET_EXTENSION:
        return PARQUET_EXTENSION, cleaned_file_name
    elif extension == SQL_EXTENSION:
        return SQL_EXTENSION, cleaned_file_name
    else:
        raise ValueError(f"Error file type for {file_name}. Only support for {CSV_EXTENSION}, {PARQUET_EXTENSION}, {JSON_EXTENSION}, {SQL_EXTENSION} files")


def read_json_file(file_path: str) -> list[dict]:
    print(file_path)
    with open(file=file_path, mode="r", encoding=ENCONDING) as file:
        data = json.load(file)
    return data


def write_json_file(data: Union[dict, list[dict]], file_path: str):
    with open(file=file_path, mode="w", encoding=ENCONDING) as file:
        json.dump(data, file)


def write_sql_file(data: str, file_path: str):
    if identify_file_type(file_path)[0] != SQL_EXTENSION:
        raise ValueError(f"Your file {file_path} is not valid. Require .sql file")
    with open(file=file_path, mode="w") as file:
        file.write(data)


def export_dict_to_json_file(data: list[dict], folder_path, file_name, write_mode=OVERWRITE_MODE[0], create_folder=False):
    folder_path = folder_path.strip().rstrip("/")
    file_type, file_name = identify_file_type(file_name)
    if file_type != JSON_EXTENSION:
        raise ValueError(f"Your file {file_name} is not valid. Require .json file")

    full_path = f"{folder_path}/{file_name}"
    write_mode = write_mode.lower()

    if write_mode.lower() not in [OVERWRITE_MODE[0], APPEND_MODE[0]]:
        raise ValueError(f"Not support {write_mode} mode. Allow write mode: {[OVERWRITE_MODE[0], APPEND_MODE[0]]}")
    
    if not check_exists_folder(folder_path) and not create_folder:
            raise Exception(f"{folder_path} is not exists")
    
    if create_folder:
        create_if_not_exist_folder(folder_path)

    # If mode append, then read old data, combine write with new data
    if write_mode == APPEND_MODE[0] and check_exists_file(full_path):
        old_data = read_json_file(full_path)
        data.extend(old_data)

    write_json_file(data, full_path)
