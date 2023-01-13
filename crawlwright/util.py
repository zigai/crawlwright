from urllib.parse import urlparse

import validators


def assert_url_valid(url: str):
    if not validators.url(url):  # type: ignore
        raise ValueError(f"URL '{url}' not valid")


def get_robots_txt_url(page_url: str):
    return urlparse(page_url).scheme + "://" + urlparse(page_url).netloc + "/robots.txt"
