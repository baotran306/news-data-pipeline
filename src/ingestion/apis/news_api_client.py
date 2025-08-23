from ingestion.apis.api_client import APIClient
from models.news import News


class NewsAPIClient(APIClient):
    MIN_TEXT_FILTER_LEN = 3
    MAX_TEXT_FILTER_LEN = 100
    CHUNK_SIZE = 1000 # Allow fetch maximum 1000 size each time 

    def __init__(self, url, api_key):
        super().__init__(url, api_key)

    def __validate_text_filter(self, text):
        if not self.MIN_TEXT_FILTER_LEN <= len(text) <= self.MAX_TEXT_FILTER_LEN:
            raise ValueError(f"Expect length text in range [{self.MIN_TEXT_FILTER_LEN},{self.MAX_TEXT_FILTER_LEN}]")
        
    def __call_request(self, endpoint, params, start_point, total_news):
        # Limit size output each request
        all_news = []
        current_length = 0
        params["number"] = min(total_news, self.CHUNK_SIZE)
        params["offset"] = start_point
        while current_length < total_news:
            respone_data = self.get(endpoint=endpoint, params=params)

            if respone_data and "news" in respone_data \
            and isinstance(respone_data["news"], list):
                ls_process_news = []

                for current_new in respone_data["news"]:
                    ls_process_news.append(News(
                        id=current_new.get("id"),
                        title=current_new.get("title"),
                        url=current_new.get("url"),
                        authors=current_new.get("authors"),
                        summary=current_new.get("summary"),
                        source_country=current_new.get("source_country"),
                        publish_time=current_new.get("publish_date")
                    ))
                all_news.extend(ls_process_news)
            
            # Update to next call request point
            params["offset"] += params["number"]
            current_length += params["number"]

        return all_news
        

    def fetch_news(self, filter_query, language="en", sort_by="publish-time", sort_direction="ASC", offset=0, number=10):
        """Search and filter news by providing information: https://worldnewsapi.com/docs/search-news/
        
        """
        self.__validate_text_filter(filter_query)
        params = {
            "text": filter_query,
            "language": language,
            "sort": sort_by,
            "sort-direction": sort_direction
        }

        return self.__call_request(endpoint="search-news", params=params, start_point=offset, total_news=number)
