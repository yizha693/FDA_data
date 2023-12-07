import logging
import requests
from config.config import Config


class Utils:
    def __init__(self):
        """Loads all attributes from the Config (as cfg) class"""
        self.config = Config()

    def make_request(self, method, url, payloads):
        if method == "GET":
            try:
                response = requests.get(url, params=payloads)
                return response
            except Exception as e:
                logging.error(f"Error while making {method} request to {url}: {e}")
            return None

    def recusrively_search_item(self, key, data):
        """serarch the key and return its corresponding value
        in a json file
        """
        results = []
        if isinstance(data, dict):
            if key in data:
                results.append(data[key])
            for k in data:
                results.extend(self.recusrively_search_item(key, data[k]))
        elif isinstance(data, list):
            for item in data:
                results.extend(self.recusrively_search_item(key, item))
        return results
