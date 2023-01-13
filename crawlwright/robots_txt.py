from urllib.parse import urlparse

import requests
from diskcache import Index
from playwright._impl._api_types import Error, TimeoutError
from protego import Protego


class BaseRobotsTxtChecker:
    def __init__(self, cache_path: str) -> None:
        ...

    def can_fetch(self, url: str, user_agent: str) -> bool:
        return True


class RobotsTxtChecker(BaseRobotsTxtChecker):
    def __init__(self, cache_path: str) -> None:
        self.cache_path = cache_path
        self.storage = Index(self.cache_path)

    def can_fetch(self, url: str, user_agent: str) -> bool:
        base_url = urlparse(url).netloc
        if robots_txt := self.storage.get(url):
            return Protego.parse(robots_txt).can_fetch(url, user_agent)
        robots_txt = requests.get(url).text
        self.storage[base_url] = robots_txt
        return Protego.parse(robots_txt).can_fetch(url, user_agent)
