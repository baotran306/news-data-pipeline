# news-data-pipeline
This project implements a data pipeline that crawls data from a public API, as well as processes the data into tabular formats. Additionally, we also provide a pipeline to exort data into CSV, Parquet, Excel file, followed by simulate upload files and generates SQL scripts for PostgreSQL. 

## Features
1. **Crawling**:
   - Fetches data from a public API: [World New API](https://worldnewsapi.com/console)
   - Saves raw data as JSON files.
2. **Processing**:
   - Reads JSON files and normalizes them into tabular format using pandas.
   - Exports data to CSV, Parquet, and Excel formats.
   - Stores processed files locally or uploads to AWS S3 (simulated locally if AWS is unavailable).
3. **ETL Pipeline**:
   - Extracts data from raw JSON files.
   - Transforms data by cleaning and normalizing schema.
   - Generates SQL UPSERT scripts for PostgreSQL.

## Project Structure
.
├── README.md
├── pyproject.toml
├── data_sample
│   ├── crawl_data.json
│   ├── merge_statement.sql
│   └── s3_local
│       ├── news_data_250824_103350
│       │   ├── csv
│       │   │   ├── _SUCCESS
│       │   │   └── part-00000-96ac73df-314c-4e38-a4f6-0eb5889a52a3-c000.csv
│       │   ├── json
│       │   │   ├── _SUCCESS
│       │   │   └── part-00000-af622f4b-5d34-4070-a62f-966a195951e4-c000.json
│       │   └── parquet
│       │       ├── _SUCCESS
│       │       └── part-00000-45101558-3af3-4b3d-9756-02ee25a6c867-c000.snappy.parquet
│       └── news_data_250824_112527
│           ├── csv
│           │   ├── _SUCCESS
│           │   └── part-00000-90e02790-3137-4e8d-9bfa-60736ca77e26-c000.csv
│           ├── json
│           │   ├── _SUCCESS
│           │   └── part-00000-321d6c23-71f7-497e-b58b-faf5eafd618f-c000.json
│           └── parquet
│               ├── _SUCCESS
│               └── part-00000-29bd1660-b7fc-4ed5-9bf5-1ce107952182-c000.snappy.parquet
├── src
│   ├── __init__.py
│   ├── conf
│   │   ├── __init__.py
│   │   ├── config.py
│   │   └── country_code_data.csv
│   ├── helpers
│   │   ├── __init__.py
│   │   ├── spark_utils.py
│   │   └── utils.py
│   ├── ingestion
│   │   ├── __init__.py
│   │   ├── apis
│   │   │   ├── __init__.py
│   │   │   ├── api_client.py
│   │   │   └── news_api_client.py
│   │   └── pipeline.py
│   ├── integration
│   │   ├── __init__.py
│   │   ├── load_postgres.py
│   │   ├── load_s3.py
│   │   └── processor.py
│   ├── main.py
│   └── models
│       ├── __init__.py
│       └── news.py
├── .pre-commit-config.yaml
└── uv.lock

## Quick start
```bash
# TODO: Add Makefile pre-install python, uv
pip install uv

# Move to src folder
cd src

# Run pipeline crawl data
uv run python3 main.py crawl-data --keywords technology \ 
--export-folder $YOUR_DESIRED_PATH/data_sample \
--export-file crawl_data.json

# Run ETL generate script to Postgres
uv run python3 main.py etl-data --target-storage postgres \
--source-file-path $YOUR_DESIRED_PATH/data_sample/crawl_data.json \
--target-file-path $OUTPUT_FOLDER_PATH/data_sample/merge_statement.sql

# Run ETL generate script to S3
uv run python3 main.py etl-data --target-storage s3 \
--source-file-path $YOUR_DESIRED_PATH/data_sample/crawl_data.json \
--target-file-path $OUTPUT_FOLDER_PATH/data_sample/s3_local
```

# CDC & AWS DMS

## Concept

### Change Data Capture(CDC)

**Change Data Capture(CDC)** is a method used to track and capture changes made to data in a database and its structure. It helps ensure that data is kept synchronized across various systems, allowing businesses to act on up-to-date information withou delay. CDC monitors and records changes in a database(insert, update, delete) as they happen. They captured changes are then transmitted to other systems or application, ensuring consistency across data sources and providing real-time insights. It can used for:
- **Data replication**: Keeping multiples systems updated with changes fom a source database or creating backups for recovery.
- **Realtime analytics**: Providing up-to-date data to analytics platforms without batch updates
- **Data migration**: Ensuring continuous data flow when migrating between different systems or databases.

### CDC in Postgres

There are several methods could be use when implementing Postgres Change Data Capture. Boardly, Postgres CDC can be divided into batch-based methods and realtime methods, some including:
- **CDC using queries(Batch):** The Query-based method involves tracking changes by querying an update timestamp column in the monitored database table that indicates the last time a row was changed. Recurring queries are made to PostgresSQL using this timestamp column to fetch all records that have been modified since the last query.
- **CDC using trigger(Batch):** As know as `Event Sourcing`, it relies on its delicated event logs as the primary source of information. Triggers are functions that automatically execute in response to operations, captures changes and written to a secondary table(changelog table), which serves as a log of modifications.
- **CDC using logs(Real-time)** Utilizing transactions log(Write Ahead Log in Postgres), which records every change made to the database and is used for transaction recording and replication. Transaction Log CDC relies on the database's transaction log as the source of truth.

## How to replicate an orders table from PostgresSQL to S3 using CDC streaming

### Overview

AWS Database Migration Service(AWS DMS) helps to extracts changes from the database transaction log generated by the database. AWS DMW then takes these changes, convert them to the target format, and applies them to the target. This process provides near real-time application to the target, reducing the complexity of replication monitoring.

### Instructions 

1. Configure Postgres for CDC
   - Suppose/Ensure that our Postgres datase has logical replication enabled.
   - Config Write Ahead Log to logical(`wal_level`)
   - Grant Database User with Replication and Read Privileges, followed by set IDENTITY for source tables(to identify rows uniquely)
   - Create a publication for source tables and perform logical decoding to convert WAL into a readable format(We can use `wal2json`)
2. Create and setup AWS DMS replication instance
    - Ensure we already have appropriate IAM permission to access AWD DMS. Go to Create replicate instace to start a database migration and specify replication instance information
    - When choose CPV, selecting the same VPC in our source or target if possible to improve performance and security, mitigate network configurations.
3. Configure source endpoint:
   - Provide connection source(host, port, crendentials...)
4. Configure target endpoint:
   - In our case is S3 as target endpoint. Create Bucket, and ensure the region that hosts our AWS DMS replication instance
5. Configure an AWS DMS target endpoint
   - Create appropriate IAM Role(Ensure both read, write, delete to S3 bucket).
   - Add DMS as a trusted entity for IAM Role
   - Create endpoint, fill our target engine as AWS S3, and the bucket name that is created in the previous step
   - We also can config endpoint setting for task behavior (file formats, partition, file sizing, S3 Partitioning ...)
6. Create an AWS DMS task
   - Choose replication instance, source, target endpoints in these previous step
   - For our case, choose **Full Load + CDC** to migrate existing data and replicate ongoing changes.
   - Map our table `orders` to S3 target, as well as confiure task to output change events as JSON file in S3
7. Run the AWS DMS tasks
   - Lauche DMS task for initial data load and continuous CDC streaming
   - Go AWD DMS Console to monitor task status, check data is written in S3 (including both table data `orders` and CDC events)

## How to replicate back from S3 to PostgreSQL batch (every hour)

### Overview

We can use AWS Glue to create to jjob read the data from S3 and load it into Postgres database in hourly batches.

### Instructions

1. Set up AWS Glue
   - To read data from S3. We create a AWS Glue crawler to catalog our `order` in S3 Bucket. We point the crawler to S3 bucket path which we use in these previous step and then run crawler job to create AWS Glue Data Catalog
   - Create a Glue ETL Job. We use the Catalog table as the source, configure the target as PostgresSQL using JDBC connection. Map the S3 data field to the `orders` table schema in Postgres. Additionally, apply transformation to filter insert, update, handle delete operation for CDC events.
2. Schedule Glue job
   - We can use Glue triggers to schedule job, for our use case need to run every hour can handle by `cron(0 * * * ? *)` means that we run at minute 0 hourly.


## Provide a sample AWS DMS JSON Config

# Orchestration and Scaling

## Designing Airflow DAG for Pipeline

Our pipeline consists of crawling, processing, and loading stages. Firstly, we run follow Crawl Data, followed by parallel run 2 jobs Upload files and Generate SQL whether Crawl Data Job successful running.

Another approach is separate two DAGs(one for Crawler one one for Processing), the downstream Processing DAG will be depend on the Crawler DAG by using `Dataset`

- Storing API Key using Airflow Variable with encrytion and fetch them to use.
- Config credentials for SSH on Airflow Connection and use their `ssh_conn_id` to connect and run command Spark Submit Job/Python Job
- Leveraging Jinja template/Params in case parse manual run with some specifics input (`--keywords economic --number 100000`)


- Pseudo Code
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.ssh import SSHOperator
from datetime import datetime
from airflow.sdk import Variable
from airflow.models.param import Param

default_args = {
    'start_date': datetime(2025, 8, 24),
    'retries': 3,
}

SSH_CONN_ID = "temp"

with DAG(
    'etl_pipeline',
    default_args=default_args,
    schedule='0 5 * * *',
    catchup=False,
    params={
        'keywords': Param(default="technology", type="str"),
        'source_file_path': Param(default="home/my_path/input", type="str"),
        'target_file_path': Param(default="home/my_path/output", type="str"),
    }
) as dag:
    crawl_data_task = SSHOperator(
        task_id='crawl_data',
        ssh_conn_id=SSH_CONN_ID,
        command="bash python_run.sh crawl-data --keywords {{ params.keywords }} --export-folder {{ params.target_file_path }} --export-file crawl_data.json"
    )
    upload_files_task = SSHOperator(
        task_id='upload_files',
        ssh_conn_id=SSH_CONN_ID,
        command="""bash deploy_spark.sh etl-data --target-storage s3
        --source-file-path {{ params.target_file_path }}/crawl_data.json
        --target-file-path {{ params.target_file_path }}/s3_local"""
    )
    generate_sql_task = SSHOperator(
        task_id='generate_sql',
        ssh_conn_id=SSH_CONN_ID,
        command="""bash deploy_spark.sh etl-data --target-storage postgres
            --source-file-path {{ params.target_file_path }}/crawl_data.json
            --target-file-path {{ params.target_file_path }}/merge_statement.sql
        """
    )

    crawl_data_task >> [upload_files_task, generate_sql_task]
```

## Optimizing Scaling up to 100M+ records

**Crawling**:

- Splitting API requests into smaller chunks, adjusts `CHUNK_SIZE` in `src/ingestion/apis/news_api_client.py`, process them in parallel/asynchronuous. However, we also need to notice the API rate limits to mitigate bans or throttling.
- Updates `cli` supports parse `offset` and some `number`, observe some metadata information like last crawled timestample, lastest offset, and store in metadata. As a result, we can start new crawl job and fetch only new or updated data, reducing redundant requests(Need to customize some logic pipeline also)
- Instead crawl data to json file and then read, process in the next step. It's better if we Kafka(or other distributed tool) to produce our crawl data in realtime into a Kafka topic. By doing this action, we enable parallel processing(broad multiple consumer) to processing downsteam

**File Processing**

- We can choose either Pandas or Pyspark to normalize schema and ETL, but it's better(like our approach ) whether we using PySpark, which support in memory processing, distributed computation across cluster. Note that we have to adjust Spark Resource through Spark Submit command/ cluster mode for more performance. Currently, our code only support run with default Spark Memory/Core Configuration in standlone mode
- Prefer Parquet instead of CSV/JSON file format for large datasets. With their columnar, and compression ability could help us reduce storage size, while ensure I/O performance, provie better approach to handle schema evolution
- Partition Parquet files by relevant columns to enable efficient querying and reduce I/O (e.g `source_country`, `date_extract` column)

**CDC Replication**

- Tuning the replication slots and write ahead logs configures, ensuere config the source database, the replicataion instace, and target system in same place, VPC if possible to ensure network bandwidth
- Tracking change by maintain last processed file timestamp in metadata store to process only deta changes, filter out ennecessary data.
- It's best choices if all tables have primary keys, reduce perform full table scan in CDC process for updates and delete operations.

**Performance and Scalability**

- Optimizing our S3 storage by use S3 lifecycle policies. For some older data, log data which is seldomly access, we can move it to cheaper storage tier (S3 Standard IA for old data like last 5 years, S3 Glacier for log data) instead S3 Standard to reduce cost.
- Monitor and turning Spark Job, optimize Spark Job by appropriate caching, resouce configurations, avoid Shuffle data
- We also could separate our to two DAG(one DAG for crawler and one DAG for processing), then setup dependency by Data-awareness using Dataset to scalable.


# TODO Notes:

- Enhance `cli`, supports more option config to Crawl Data efficiently.
- Add Makefile/Dockerfile supports pre-install dependencies, environment, build image
- Create script support Spark Submit with configurable resouces.
- Refactor codebase: 
  - Enhance Docstring for clearer desciption, input/output, usage
  - Create Class for Json Operation, Json Encoder(compatible Datatime ISO Format)
  - Create Interface for I/O Operation(Read file, write file/folder, valid path, check exists ...)
- Add Pytests
- Add CI Files for Validation, Test, Lint Code
