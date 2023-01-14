import asyncio
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
from urllib.parse import urljoin

from aiolimiter import AsyncLimiter
from parsel import Selector
from playwright._impl._api_types import Error, TimeoutError
from stdl.dt import Timer
from wrighter import AsyncWrighter, Plugin, WrighterOptions
from wrighter.async_wrighter import Page
from wrighter.plugin import Plugin

from crawlwright.options import CrawlerOptions, load_crawler_opts
from crawlwright.request_queue import CrawlRequest, RequestQueue
from crawlwright.robots_txt import BaseRobotsTxtChecker, RobotsTxtChecker


@dataclass
class CrawlerPage:
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
        self.log.debug("Loading request queue...")
        self.queue = RequestQueue(self.internal_dir("requests_storage"))
        self.log.debug("Loading ROBOTS.txt checker...")
        self._robots_checker = (
            RobotsTxtChecker(self.internal_dir("robots_storage"))
            if self.crawler_opts.obey_robots_txt
            else BaseRobotsTxtChecker(self.internal_dir("robots_storage"))
        )
        self._crawlers: list[Page] = ...  # type: ignore
        self._lock = threading.Lock()
        self._limiter = AsyncLimiter(*self.crawler_opts.request_limit)
        self._crawlers: list[CrawlerPage] = []

    async def init(self):
        ...

    async def run(self):
        await self.start()
        await self.init()
        timer = Timer(ms=False)
        await self.initialize_crawlers()
        crawl_tasks = [asyncio.create_task(self.crawl(i)) for i in self._crawlers]
        await asyncio.gather(*crawl_tasks)
        time = timer.stop()
        self.log.info(f"Crawling finished in {time}.")
        for task in crawl_tasks:
            task.cancel()

    def urljoin(self, url: str, base: str | None = None) -> str:
        if base:
            return urljoin(base, url)
        return urljoin(self.base_url, url)  # type: ignore

    async def initialize_crawlers(self):
        for i in range(self.crawler_opts.concurrency):
            self._crawlers.append(CrawlerPage(i, await self.context.new_page()))

    def _add_request(self, requests: CrawlRequest):
        with self._lock:
            self.queue.add(requests)

    def _add_requests(self, requests: list[CrawlRequest]):
        with self._lock:
            self.queue.extend(requests)

    def add_request(self, url: str, label: str | None = None, referer: str | None = None):
        self._add_request(CrawlRequest(url, label, 0, referer))

    def add_requests(self, urls: list[str], label: str | None = None, referer: str | None = None):
        with self._lock:
            requests = [CrawlRequest(i, label, 0, referer) for i in urls]
            self.queue.extend(requests)

    def can_fetch(self, url: str) -> bool:
        can_fetch = self._robots_checker.can_fetch(url, self.options.user_agent)  # type: ignore
        if not can_fetch:
            self.log.warning(
                f"Crawling '{url}' is forbidden by ROBOTS.txt",
                obey_robots_txt=self.crawler_opts.obey_robots_txt,
            )
        return can_fetch

    async def crawl(self, crawler: CrawlerPage):
        page = crawler.page
        while not self.queue.empty:
            request = self.queue.pop()
            if not self.can_fetch(request.url):
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
            await self._handle_request(page, request.label)
        self.log.info(f"Crawler {crawler._id} finished")

    async def _handle_request(self, page: Page, label: str | None):
        self.log.info(f"Processing page", url=page.url, label=label)
        html = await page.content()
        selector = Selector(text=html)
        await self.extract_links(selector, page, label)
        await self.parse_page(selector, page, label)

    async def _handle_failed_request(self, request: CrawlRequest):
        if request.retries == self.crawler_opts.max_retries:
            self.queue.failed[request.url] = None
            self.log.warning(f"Max retries reached for {request.url}")
            return
        else:
            request.retries += 1
            self._add_request(request)

    async def extract_links(self, selector: Selector, page: Page, label: str | None):
        links = selector.css("a::attr(href)").getall()
        requests = [
            CrawlRequest(url=self.urljoin(link), label=label, referer=self.base_url)
            for link in links
        ]
        self.log.info(f"Extracted {len(requests)} links from {page.url}")
        self._add_requests(requests)

    async def parse_page(self, selector: Selector, page: Page, label: str | None):
        ...


__all__ = ["Crawlwright", "CrawlerOptions", "WrighterOptions"]
