import asyncio
import sys
import threading
from pathlib import Path
from typing import Any, Mapping

from aiolimiter import AsyncLimiter
from playwright._impl._api_types import Error, TimeoutError
from pydantic import BaseModel, validator

from request_queue import RequestQueue
from wrighter import AsyncWrighter, Plugin, WrighterOptions
from wrighter.async_wrighter import Page
from wrighter.options import BaseOptions


class CrawlerOptions(BaseOptions):

    concurrency: int = 5
    request_limit: tuple = (30, 1)
    max_retries: int = 1

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


class Crawlwright(AsyncWrighter):
    def __init__(
        self,
        crawler_opts: str | Path | Mapping | None | CrawlerOptions = None,
        wrighter_opts: str | Path | Mapping | None | WrighterOptions = None,
        plugins: list[Plugin] | None = None,
    ) -> None:
        super().__init__(wrighter_opts, plugins)
        self.crawler_opts = load_crawler_opts(crawler_opts)
        self.crawlers: list[Page] = ...  # type: ignore
        self.queue = RequestQueue(self.options.data_dir)
        self.lock = threading.Lock()
        self.limiter = AsyncLimiter(*self.crawler_opts.request_limit)

    async def start(self):
        await super().start()
        self.crawlers = [
            await self.context.new_page() for _ in range(self.crawler_opts.concurrency)
        ]
        crawl_tasks = [asyncio.create_task(self.crawl(i)) for i in self.crawlers]
        await asyncio.gather(*crawl_tasks)
        self.log.info("Crawling finished")
        for c in crawl_tasks:
            c.cancel()

    def add_request(self, url: str, label: str | None = None):
        with self.lock:
            self.queue.add({"url": url, "label": label, "retries": 0})

    def add_requests(self, urls: list[str], label: str | None = None):
        with self.lock:
            requests = [{"url": i, "label": label, "retries": 0} for i in urls]
            self.queue.extend(requests)

    async def crawl(self, page: Page):
        while not self.queue.empty:
            request = self.queue.pop()
            async with self.limiter:  # type: ignore
                try:
                    await page.goto(request["url"])
                    await page.wait_for_load_state()
                except (TimeoutError, Error) as e:
                    self.log.warning(f"Failed to load {request['url']}")
                    self.log.error(e)
                    await self._handle_failed_request(request)
                    continue
            await self._process_page(page, request["label"])

    async def _handle_failed_request(self, request: dict):
        if request["retries"] == self.crawler_opts.max_retries:
            self.queue.failed[request["url"]] = None
            self.log.warning(f"Max retries reached for {request['url']}")
            return
        else:
            request["retries"] += 1
            self.queue.requests[request["url"]] = request

    async def _process_page(self, page: Page, label: str):
        self.log.info(f"Processing {page.url}", label=label)
        await self.process_page(page, label)

    async def process_page(self, page: Page, label: str):
        await self.sleep(1)


async def main():
    urls = [f"http://books.toscrape.com/catalogue/page-{p}.html" for p in range(1, 51)]
    # urls[0] = "http://books.toscrape.com/catalogue/page-{}{}{}{1.html"
    urls[0] = "http://www.gogl.com/"
    o = WrighterOptions(headless=False)
    crawler = Crawlwright(wrighter_opts=o)
    crawler.queue.clear()
    crawler.print_configuration()
    crawler.add_requests(urls, label="books")
    await crawler.start()


if __name__ == "__main__":
    asyncio.run(main())
