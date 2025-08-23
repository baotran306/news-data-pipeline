import click

@click.group()
def main_cli():
    pass

@click.command("crawl-data")
@click.option("--keywords", type=str, required=True)
@click.option("--export-folder", type=str, required=True)
@click.option("--export-file", type=str, required=True)
def run_crawl_cli(keywords, export_folder, export_file):
    from ingestion.pipeline import CrawlNewPipeline
    from conf.config import get_api_key, NEWS_API_URL

    pipeline = CrawlNewPipeline(api_url=NEWS_API_URL, api_key=get_api_key())
    pipeline.run(keywords=keywords, export_folder=export_folder, export_file_name=export_file, number=15)
    print("Done")
    # TODO: Need to return offset incase rerun from a specific time


@click.command("etl-data")
@click.option("--source-file-path", type=str)
@click.option("--target-file-path", type=str)
@click.option("--target-storage", type=click.Choice(["s3", "postgres"]), default="s3")
def run_processing_cli(source_file_path, target_file_path, target_storage):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    params = {
        "spark": spark,
        "json_input_path": source_file_path.rstrip(),
        "output_path": target_file_path.rstrip()
    }

    if target_storage == "s3":
        from integration.load_s3 import LoadS3Pipeline
        pipeline = LoadS3Pipeline(**params)
    else:
        from integration.load_postgres import LoadPostgresPipeline
        pipeline = LoadPostgresPipeline(**params)

    pipeline.run()
    spark.stop()


main_cli.add_command(run_crawl_cli)
main_cli.add_command(run_processing_cli)

if __name__ == "__main__":
    main_cli()
