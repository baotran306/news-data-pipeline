import os

NEWS_API_URL = "https://api.worldnewsapi.com/"
NEWS_API_KEY_VARIABLE = "NEWS_API_KEY"


def get_api_key():
    api_key = os.getenv(NEWS_API_KEY_VARIABLE)
    if api_key:
        return api_key
    else:
        raise ValueError(f"Cann't extract {NEWS_API_KEY_VARIABLE}")
