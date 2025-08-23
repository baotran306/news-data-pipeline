from integration.processor import ETLPipeline
from pyspark.sql import DataFrame
from helpers import write_sql_file

class LoadPostgresPipeline(ETLPipeline):
    def load_to_target(self, df: DataFrame):
        upsert_script = self.generate_script_upsert(df)
        write_sql_file(upsert_script, self.output_path)

    @staticmethod
    def generate_script_upsert(df: DataFrame):
        """Upsert to DB target in Postgres
        Assume that Postgres version support MERGE Statement
        and the table's structure like below:
            |-- authors: varchar(100) NULL
            |-- id: numeric NOT NULL PRIMARY KEY
            |-- publish_time: timestamp NULL
            |-- source_country: varchar(2) NULL
            |-- summary: nvarchar(50000) NULL
            |-- title: nvarchar(2000) NULL
            |-- url: varchar(500) NULL
            |-- source_country_name: varchar(100) NULL
            |-- updated: timestamp NOT NULL
        
        """
        df.createOrReplaceTempView("source_table")
        upsert_script = """
MERGE INTO table_target as t
USING source_table as s
ON t.id = s.id
WHEN MATCHED THEN
    UPDATE SET 
        authors = s.authors,
        publish_time = s.publish_time,
        summary = s.summary,
        source_country = s.source_country,
        source_country_name = s.source_country_name,
        updated = CURRENT_TIMESTAMP(),
        title = s.title,
        url = s.url
WHEN NOT MATCHED THEN
    INSERT(id, title, summary, authors, url, source_country, source_country_name, authors, updated)
    VALUES(s.id, s.title, s.summary, s.authors, s.url, s.source_country, s.source_country_name, s.authors, CURRENT_TIMESTAMP())
"""
        return upsert_script