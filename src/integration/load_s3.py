from datetime import datetime
from pyspark.sql import DataFrame

from helpers import (
    create_temp_folder,
    dataframe_to_csv,
    dataframe_to_json,
    dataframe_to_parquet,
    remove_folder,
    upload_folder,
)
from integration.processor import ETLPipeline


class LoadS3Pipeline(ETLPipeline):
    def load_to_target(self, df: DataFrame):
        """Simulate with a local folder instead AWS
        In case using AWS, use boto3 library
        """
        temp_data_folder = self.export_data(df)

        upload_folder(folder_source=temp_data_folder, folder_target=self.output_path)
        # Remove temp folder after processing done
        remove_folder(temp_data_folder)

    def export_data(self, df: DataFrame) -> str:
        """Export data to target(in this case is a temp folder - intermediate layer before uploading S3)
        
        """
        # Generate a name by timestamp
        subname = f"news_data_{datetime.now().strftime('%y%m%d_%H%M%S')}"
        folder_temp = create_temp_folder(subname)

        # Cache dataframe to avoid multiple reprocess due to writing multiple file formats
        # Can use different demand Storage Level base on dataset size and Spark Memory Configurations
        df = df.persist()
        dataframe_to_csv(df=df, output_path=f"{folder_temp}/csv")
        dataframe_to_json(df=df, output_path=f"{folder_temp}/json")
        dataframe_to_parquet(df=df, output_path=f"{folder_temp}/parquet")
        df.unpersist()

        return folder_temp
