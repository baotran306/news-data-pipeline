from ingestion.apis.news_api_client import NewsAPIClient
from helpers.utils import export_dict_to_json_file, settup_logger

class CrawlNewPipeline():
    def __init__(self, api_url, api_key):
        self.logger = settup_logger(__name__)
        self.news_client = NewsAPIClient(url=api_url, api_key=api_key)

    def run(self):
        data = self.news_client.fetch_news(filter_query="technology")
        for piece in data:
            self.logger.info(piece)
        export_dict_to_json_file(data=[piece.as_dict() for piece in data], folder_path="/home/cisco306/dev/news-data-pipeline/src/data", file_name="abc.json", write_mode="overwrite", create_folder=True)
        # save to json