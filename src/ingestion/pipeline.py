from ingestion.apis.news_api_client import NewsAPIClient
from helpers.utils import export_dict_to_json_file, settup_logger

class CrawlNewPipeline():
    def __init__(self, api_url, api_key):
        self.logger = settup_logger(__name__)
        self.news_client = NewsAPIClient(url=api_url, api_key=api_key)

    def run(self, keywords, export_folder, export_file_name, **kwargs):
        data = self.news_client.fetch_news(filter_query=keywords, **kwargs)
        for piece in data:
            self.logger.info(piece)
        export_dict_to_json_file(data=[piece.as_dict() for piece in data], folder_path=export_folder, file_name=export_file_name, write_mode="append", create_folder=True)
        # save to json