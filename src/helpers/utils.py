import json
import logging
import os
import subprocess
from datetime import datetime
from typing import Union

DATETIME_FMT = "%Y-%m-%d %H:%S:%M"
ENCONDING = "utf-8"

DOT_CHAR = "."
JSON_EXTENSION = "json"
PARQUET_EXTENSION = "parquet"
CSV_EXTENSION = "csv"
SQL_EXTENSION = "sql"
APPEND_MODE = "append"
OVERWRITE_MODE = "overwrite"


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
def parse_iso_datetime(string_time: str) -> datetime:
    """Parse datatime string to datetime type - Ensure compatible to datetime JSON file in ISO format"""
    try:
        dtime = datetime.strptime(string_time, DATETIME_FMT).isoformat()
        return dtime
    except ValueError:
        return None


def check_exists_file(file_path: str) -> bool:
    """Check whether a input file path exists"""
    # Only rstrip to handle error whether user using relative path
    file_path = file_path.rstrip()
    if os.path.isfile(file_path):
        return True
    else:
        return False


def check_exists_folder(folder_path: str) -> bool:
    """Check whether a input file path exists"""
    folder_path = folder_path.rstrip()
    if os.path.isdir(folder_path):
        return True
    else:
        return False


def create_if_not_exist_folder(folder_path: str) -> bool:
    """Create folder whether it is not exists"""
    if not check_exists_folder(folder_path):
        os.makedirs(folder_path)


def create_temp_folder(folder_name: str) -> str:
    folder_path = f"/tmp/{folder_name}"
    create_if_not_exist_folder(folder_path)
    return folder_path


def remove_folder(folder_path: str):
    """Remove folder and all their descendant"""
    try:
        if check_exists_folder(folder_path):
            subprocess.run(["rm", "-rf", folder_path], check=True)
    except subprocess.CalledProcessError as e:
        raise Exception(f"Error remove folder: {e}")


def upload_folder(folder_source: str, folder_target: str):
    """Upload folder including all their descendant to specific target folder"""
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
        raise ValueError(
            f"Error file type for {file_name}. Only support for {CSV_EXTENSION}, {PARQUET_EXTENSION}, {JSON_EXTENSION}, {SQL_EXTENSION} files"
        )


def read_json_file(file_path: str) -> list[dict]:
    """Read Json file with utf-8 encoding"""
    with open(file=file_path, mode="r", encoding=ENCONDING) as file:
        data = json.load(file)
    return data


def write_json_file(data: Union[dict, list[dict]], file_path: str):
    """Write Json file with utf-8 encoding, preserve non-ASCII characters"""
    with open(file=file_path, mode="w", encoding=ENCONDING) as file:
        # Set False to preserving non-ASCII characters
        json.dump(data, file, ensure_ascii=False)


def write_sql_file(data: str, file_path: str):
    """Read string to SQL File"""
    if identify_file_type(file_path)[0] != SQL_EXTENSION:
        raise ValueError(f"Your file {file_path} is not valid. Require .sql file")
    with open(file=file_path, mode="w") as file:
        file.write(data)


def export_dict_to_json_file(
    data: list[dict], folder_path: str, file_name: str, write_mode: str = OVERWRITE_MODE, create_folder: str = False
):
    folder_path = folder_path.strip().rstrip("/")
    file_type, file_name = identify_file_type(file_name)
    if file_type != JSON_EXTENSION:
        raise ValueError(f"Your file {file_name} is not valid. Require .json file")

    full_path = f"{folder_path}/{file_name}"
    write_mode = write_mode.lower()

    if write_mode.lower() not in [OVERWRITE_MODE, APPEND_MODE]:
        raise ValueError(f"Not support {write_mode} mode. Allow write mode: {[OVERWRITE_MODE, APPEND_MODE]}")

    if not check_exists_folder(folder_path) and not create_folder:
        raise Exception(f"{folder_path} is not exists")

    if create_folder:
        create_if_not_exist_folder(folder_path)

    # If mode append, then read old data, combine write with new data
    if write_mode == APPEND_MODE and check_exists_file(full_path):
        old_data = read_json_file(full_path)
        data.extend(old_data)

    write_json_file(data, full_path)
