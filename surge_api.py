import requests
from urllib.parse import urljoin
import os
from typing import TypedDict


class RecentResponse(TypedDict):
    requests: list[dict]


class ActiveResponse(TypedDict):
    requests: list[dict]


class SurgeAPI:
    def __init__(self, api_base: str | None = None, api_key: str | None = None):
        super().__init__()
        if api_base is None:
            api_base = os.environ.get('SURGE_API_BASE')
        if api_key is None:
            api_key = os.environ.get('SURGE_API_KEY')
        self.api_base = api_base
        self.session = requests.Session()
        self.session.headers = {'x-key': api_key}

    def recent(self) -> RecentResponse:
        url = urljoin(self.api_base, '/v1/requests/recent')
        resp = self.session.get(url)
        return resp.json()

    def active(self) -> ActiveResponse:
        url = urljoin(self.api_base, '/v1/requests/active')
        resp = self.session.get(url)
        return resp.json()
