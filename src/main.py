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


@click.command("processing")
@click.option("--file")
def run_processing_cli(file):
    pass


main_cli.add_command(run_crawl_cli)
main_cli.add_command(run_processing_cli)

if __name__ == "__main__":
    main_cli()
