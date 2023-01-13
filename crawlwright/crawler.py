import asyncio
import threading
from pathlib import Path
from typing import Any, Mapping

from aiolimiter import AsyncLimiter
from options import CrawlerOptions, load_crawler_opts
from playwright._impl._api_types import Error, TimeoutError
from request_queue import CrawlRequest, RequestQueue
from robots_txt import BaseRobotsTxtChecker, RobotsTxtChecker
from stdl.dataclass import dataclass
from stdl.dt import Timer
from stdl.fs import path_join
from wrighter import AsyncWrighter, Plugin, WrighterOptions
from wrighter.async_wrighter import Page


@dataclass
class Crawler:
    _id: int
    page: Page
    failed_requests: int = 0
    total_requests: int = 0

    def request_success(self):
        self.total_requests += 1

    def request_failed(self):
        self.total_requests += 1
        self.failed_requests += 1


class Crawlwright(AsyncWrighter):
    def __init__(
        self,
        crawler_opts: str | Path | Mapping | None | CrawlerOptions = None,
        wrighter_opts: str | Path | Mapping | None | WrighterOptions = None,
        plugins: list[Plugin] | None = None,
    ) -> None:
        super().__init__(wrighter_opts, plugins)
        self.crawler_opts = load_crawler_opts(crawler_opts)
        self.queue = RequestQueue(
            path_join(self.options.data_dir, "request_storage")  # type:ignore
        )
        self._crawlers: list[Page] = ...  # type: ignore
        self._lock = threading.Lock()
        self._limiter = AsyncLimiter(*self.crawler_opts.request_limit)
        self._crawlers: list[Crawler] = []
        rt_dir = self._media_dir("robots_storage")
        self._robots_checker = (
            RobotsTxtChecker(rt_dir)
            if self.crawler_opts.obey_robots_txt
            else BaseRobotsTxtChecker(rt_dir)
        )

    async def _init_crawlers(self):
        for i in range(self.crawler_opts.concurrency):
            self._crawlers.append(Crawler(i, await self.context.new_page()))

    async def start(self):
        await super().start()
        t = Timer(ms=False)
        await self._init_crawlers()
        crawl_tasks = [asyncio.create_task(self.crawl(i)) for i in self._crawlers]
        await asyncio.gather(*crawl_tasks)
        time = t.stop()
        self.log.info(f"Crawling finished in {time}.")
        for c in crawl_tasks:
            c.cancel()

    def _add_request(self, req: CrawlRequest):
        with self._lock:
            self.queue.add(req)

    def add_request(self, url: str, label: str | None = None, referer: str | None = None):
        self._add_request(CrawlRequest(url, label, 0, referer))

    def add_requests(self, urls: list[str], label: str | None = None, referer: str | None = None):
        with self._lock:
            requests = [CrawlRequest(i, label, 0, referer) for i in urls]
            self.queue.extend(requests)

    async def crawl(self, crawler: Crawler):
        page = crawler.page
        while not self.queue.empty:
            request = self.queue.pop()
            if not self._robots_checker.can_fetch(request.url, self.options.user_agent):
                self.log.warning(
                    f"Crawling '{request.url}' is forbidden by ROBOTS.txt", obey_robots_txt=True
                )
                continue
            async with self._limiter:  # type: ignore
                try:
                    await page.goto(request.url, referer=request.referer)
                    await page.wait_for_load_state()
                except (TimeoutError, Error) as e:
                    self.log.warning(f"Failed to load {request.url}")
                    self.log.error(e)
                    crawler.request_failed()
                    await self._handle_failed_request(request)
                    continue
            crawler.request_success()
            await self._process_page(page, request.label)

    async def _handle_failed_request(self, request: CrawlRequest):
        if request.retries == self.crawler_opts.max_retries:
            self.queue.failed[request.url] = None
            self.log.warning(f"Max retries reached for {request.url}")
            return
        else:
            request.retries += 1
            self._add_request(request)

    async def _process_page(self, page: Page, label: str | None):
        self.log.info(f"Processing {page.url}", label=label)
        await self.process_page(page, label)

    async def process_page(self, page: Page, label: str | None):
        ...


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
