from pathlib import Path
from typing import Any, Mapping

from pydantic import validator
from wrighter.options import BaseOptions, WrighterOptions


class CrawlerOptions(BaseOptions):
    concurrency: int = 5
    request_limit: tuple = (30, 1)  # 30 requests per second
    max_retries: int = 3
    obey_robots_txt: bool = False

    @validator("concurrency")
    def __validate_concurrency(cls, v):
        if v < 1:
            raise ValueError("concurrency must be greater than 0")
        return v


def load_crawler_opts(
    opts: str | Path | Mapping[str, Any] | None | CrawlerOptions
) -> CrawlerOptions:
    """
    - If the input is `None`, returns a default `CrawlerOptions` object.
    - If the input is a string or `Path`, returns a `CrawlerOptions` object constructed from the parse file.
    - If the input is a mapping, returns a `CrawlerOptions` object constructed from the mapping.
    - If the input is already a `CrawlerOptions` object, returns the object itself.
    - Otherwise, raises a `TypeError`.
    """
    if opts is None:
        return CrawlerOptions()
    elif isinstance(opts, (str, Path)):
        return CrawlerOptions.parse_file(opts)
    elif isinstance(opts, Mapping):
        return CrawlerOptions(**opts)
    elif isinstance(opts, CrawlerOptions):
        return opts
    raise TypeError(type(opts))


__all__ = ["CrawlerOptions", "load_crawler_opts"]
