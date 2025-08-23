from abc import ABC

import requests

from helpers.utils import settup_logger

logger = settup_logger(__name__)


class APIClient(ABC):
    """Interface API Client"""

    def __init__(self, url, api_key=None):
        self.__url_base = self.clean_url(url)
        self.__api_key = api_key
        self.params = {
            "api-key": api_key  # Required
        }
        self.headers = {"Content-Type": "application/json"}

    @property
    def url_base(self):
        return self.__url_base

    @url_base.setter
    def url_base(self, url):
        self.__url_base = self.clean_url(url)

    @property
    def api_key(self):
        raise PermissionError("API Key is write-only")

    @api_key.setter
    def api_key(self, api_key):
        self.__api_key = str(api_key)
        self.params["api-key"] = self.__api_key

    @staticmethod
    def clean_url(url_text):
        """Remove redundant space and slash"""
        if isinstance(url_text, str):
            url_text = url_text.strip(" ").strip("/")
            return url_text
        else:
            raise TypeError(f"Require string URL. Your URL='{url_text}' is {type(url_text)}")

    def _make_request(self, method, endpoint, params={}, headers={}, data=None, json=None):
        url = f"{self.url_base}/{endpoint.lstrip('/')}"
        # Add additional params information
        if params:
            self.params.update(params)

        try:
            respone = requests.request(
                method=method, url=url, params=self.params, headers=self.headers, data=data, json=json
            )
            # Raise if found error
            respone.raise_for_status()

            if respone.content:
                return respone.json()
            else:
                return {}
        except requests.exceptions.RequestException as e:
            logger.error(f"Error during {method} request to {url}: {e}")
            return None

    def get(self, endpoint, params={}, headers={}):
        return self._make_request("GET", endpoint=endpoint, params=params)

    def post(self, endpoint, params={}, headers={}, json=None):
        return self._make_request("GET", endpoint=endpoint, params=params, json=json)

    def put(self, endpoint, params={}, headers={}, data=None, json=None):
        return self._make_request("GET", endpoint=endpoint, params=params, data=data, json=json)
