import os
from pathlib import Path
from pprint import pp, pprint
from typing import Any, Iterable

import validators
from diskcache import Deque, Index
from loguru import logger as log
from stdl.dt import Timer
from stdl.fs import SEP


def assert_url_valid(url: str):
    if not validators.url(url):  # type: ignore
        raise ValueError(f"URL '{url}' not valid")


def make_path(*parts):
    """
    Example:
        make_path("a", "b", "c") -> "a/b/c" (on POSIX)
    """
    return SEP.join(parts)


class RequestQueue:
    def __init__(self, directory: str | Path) -> None:
        self.directory = os.path.abspath(str(directory))
        self._requests_dir = make_path(self.directory, "requests")
        self._failed_dir = make_path(self.directory, "failed")
        self._complete_dir = make_path(self.directory, "complete")
        self.requests = Index(self._requests_dir)
        self.failed = Index(self._failed_dir)
        self.complete = Index(self._complete_dir)
        self.urls = self.get_all_urls()

    def __repr__(self) -> str:
        return f"RequestQueue(size={self.size}, directory='{self.directory}')"

    def __len__(self):
        return len(self.requests.items())

    @property
    def size(self):
        return len(self)

    @property
    def items(self) -> list[dict]:
        return [i for i in self.requests.keys()]

    @property
    def empty(self) -> bool:
        return len(self) == 0

    def get_all_urls(self) -> set[str]:
        return {url for url in [*self.requests.keys(), *self.failed.keys(), *self.complete.keys()]}

    def add(self, request: dict):
        if request["url"] in self.urls:
            return
        assert_url_valid(request["url"])
        self.requests[request["url"]] = request
        self.urls.add(request["url"])

    def extend(self, requests: Iterable[dict]):
        for i in requests:
            self.add(i)

    def pop(self) -> dict[str, Any]:
        item = self.requests.popitem(last=False)
        return item[1]  # type:ignore

    def clear(self):
        self.urls.clear()
        self.requests.clear()
        self.failed.clear()
        self.complete.clear()
