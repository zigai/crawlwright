import os
from pathlib import Path
from typing import Iterable

from diskcache import Index
from stdl.dataclass import Data, dataclass
from stdl.fs import joinpath
from util import assert_url_valid


@dataclass()
class CrawlRequest(Data):
    url: str
    label: str | None
    retries: int = 0
    referer: str | None = None


class RequestQueue:
    def __init__(self, directory: str | Path) -> None:
        self.directory = os.path.abspath(str(directory))
        self._request_cache_dir = joinpath(self.directory, "requests")
        self._failed_cache_dir = joinpath(self.directory, "failed")
        self._complete_cache_dir = joinpath(self.directory, "complete")
        self.requests = Index(self._request_cache_dir)
        self.failed = Index(self._failed_cache_dir)
        self.complete = Index(self._complete_cache_dir)
        self.urls = self.get_all_urls()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(size={self.size}, directory='{self.directory}')"

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

    def add(self, request: CrawlRequest):
        if request.url in self.urls:
            return
        assert_url_valid(request.url)
        self.requests[request.url] = request.dict
        self.urls.add(request.url)

    def extend(self, requests: Iterable[CrawlRequest]):
        for i in requests:
            self.add(i)

    def pop(self) -> CrawlRequest:
        item = self.requests.popitem(last=False)
        return CrawlRequest(**item[1])  # type:ignore

    def clear(self):
        self.urls.clear()
        self.requests.clear()
        self.failed.clear()
        self.complete.clear()


__all__ = ["CrawlRequest", "RequestQueue"]
