from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from helpers.utils import check_exists_file, settup_logger, create_if_not_exist_folder, identify_file_type

logger = settup_logger(__name__)


def read_data_parquet(spark: SparkSession, path: str, read_options: Optional[dict] = {}) -> DataFrame:
    if check_exists_file(path):
        return spark.read.parquet(path, **read_options)
    else:
        raise FileNotFoundError(f"File not found {path}")


def read_data_json(
    spark: SparkSession, path: str, schema: Optional[StructType] = None, read_options: Optional[dict] = {}
) -> DataFrame:
    if check_exists_file(path):
        if schema:
            return spark.read.schema(schema).json(path, **read_options)
        else:
            return spark.read.json(path, **read_options)
    else:
        raise FileNotFoundError(f"File not found {path}")


def read_data_csv(
    spark: SparkSession, path: str, schema: Optional[StructType] = None, read_options: Optional[dict] = {}
) -> DataFrame:
    if check_exists_file(path):
        if schema:
            return spark.read.schema(schema).json(path, **read_options)
        else:
            return spark.read.csv(path, **read_options)
    else:
        raise FileNotFoundError(f"File not found {path}")


def dataframe_to_parquet(
    df: DataFrame, output_path: str, write_mode: str = "error", write_options: Optional[dict] = {}
):
    df.write.parquet(path=output_path, mode=write_mode, **write_options)
    logger.info(f"Write data succefully to {output_path}")


def dataframe_to_csv(df: DataFrame, output_path: str, write_mode: str = "error", write_options: Optional[dict] = {}):
    df.write.csv(path=output_path, mode=write_mode, **write_options)
    logger.info(f"Write data succefully to {output_path}")


def dataframe_to_json(df: DataFrame, output_path: str, write_mode: str = "error", write_options: Optional[dict] = {}):
    df.write.json(path=output_path, mode=write_mode, **write_options)
    logger.info(f"Write data succefully to {output_path}")

def dataframe_to_excel(df: DataFrame, output_path: str,  write_mode: str = "error", file_name: str = "data.xlsx", write_options: Optional[dict] = {}):
    excel_supported_extension = {"xlsx", "xls"}
    full_path = f"{output_path}/{file_name}"
    if identify_file_type(full_path)[0] not in excel_supported_extension:
        raise ValueError(f"Your file {full_path} is not valid. Require {excel_supported_extension} file")
    
    create_if_not_exist_folder(output_path)

    if check_exists_file(full_path) and write_mode.lower() == "error":
        raise Exception(f"File {full_path} already exists, change write mode to overwrite")
    else:  # Overwrite always
        df.pandas_api().to_excel(full_path, **write_options)
    logger.info(f"Write data succefully to {output_path}")
