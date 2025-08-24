from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from helpers import read_data_csv, read_data_json


class ETLPipeline(ABC):
    def __init__(self, spark: SparkSession, json_input_path: str, output_path: str):
        self.spark = spark
        self.json_input_path = json_input_path
        self.output_path = output_path
        self.country_csv_path = "conf/country_code_data.csv"

    def __get_data_country_code(self) -> DataFrame:
        csv_options = {"header": True, "inferSchema": True}
        df = read_data_csv(self.spark, self.country_csv_path, read_options=csv_options)
        # Rename column to lower case
        ls_col = df.columns
        for col in ls_col:
            df = df.withColumnRenamed(col, col.lower())

        # Enforce code to lower, using join in the next step
        df = df.withColumn("code", f.lower(f.col("code")))
        return df

    def json_to_dataframe(self) -> DataFrame:
        json_read_options = {"multiLine": True}
        df = read_data_json(self.spark, self.json_input_path, read_options=json_read_options)
        return df

    def transform_data(self, df: DataFrame) -> DataFrame:
        # Remove NULL id(represent by -1) and then deduplicate by id
        df = df.filter(f.col("id") != -1).dropDuplicates(["id"])

        # Format authors
        df = (
            df.withColumn("authors", f.array_join(f.col("authors"), ","))
            .withColumn("title", f.upper(f.col("title")))
            .withColumn("publish_time", f.to_timestamp(f.col("publish_time")))
            .withColumn("source_country", f.lower(f.col("source_country")))
        )

        df_country_code = self.__get_data_country_code()

        # Broadcast join small dataset
        df = (
            df.alias("fact")
            .join(
                f.broadcast(df_country_code).alias("dim"),
                on=(f.col("fact.source_country") == f.col("dim.code")),
                how="left",
            )
            .select(f.col("fact.*"), f.coalesce(f.col("dim.name"), f.lit("Not Given")).alias("source_country_name"))
        )

        return df

    @abstractmethod
    def load_to_target(self, df: DataFrame):
        pass

    def run(self):
        df = self.json_to_dataframe()
        df = self.transform_data(df)
        df.printSchema()
        self.load_to_target(df)
