from ingestion.pipeline import CrawlNewPipeline
from conf.config import get_api_key, NEWS_API_URL


if __name__ == "__main__":
    pipeline = CrawlNewPipeline(api_url=NEWS_API_URL, api_key=get_api_key())
    pipeline.run()
    print("Done")
    # for index, new in enumerate(news):
    #     print(index)
    #     print(new)
    #     print("====")
