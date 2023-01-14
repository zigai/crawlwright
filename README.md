# Crawlwright

# Installation
#### From source
```
pip install git+https://github.com/zigai/crawlwright.git
```

# Example
Example crawler for [books.toscrape.com](books.toscrape.com)
```python
import asyncio
from parsel import Selector
from crawlwright import CrawlerOptions, Crawlwright, Page, WrighterOptions


class BooksToScrapeCrawler(Crawlwright):
    async def init(self):
        self.base_url = "http://books.toscrape.com/catalogue/"
        self.add_requests(
            [f"{self.base_url}page-{p}.html" for p in range(1, 10)], label="book_list"
        )

    async def extract_links(self, selector: Selector, page: Page, label: str | None):
        if label == "book_list":
            books = selector.xpath("//h3/a/@href").getall()
            books = [self.urljoin(book) for book in books]
            self.add_requests(books, label="book_page")
            next_page_url = selector.xpath('//a[text()="next"]/@href').get()
            if next_page_url:
                self.add_request(self.urljoin(next_page_url), label="book_list")

    async def parse_page(self, selector: Selector, page: Page, label: str | None):
        if label == "book_page":
            price = selector.css("p.price_color:nth-child(2)::text").get()
            title = selector.css("h1::text").get()
            description = selector.css(".product_page > p:nth-child(3)::text").get()
            data = {
                "title": title,
                "price": price,
                "description": description,
            }
            print(data)
            ...

async def main():
    wrighter_opts = WrighterOptions(headless=False, user_agent="Crawlwright")
    opts = CrawlerOptions(concurrency=5, obey_robots_txt=True, max_retries=3)
    await BooksToScrapeCrawler(opts, wrighter_opts).run()

if __name__ == "__main__":
    asyncio.run(main())

```
# License
[MIT License](https://github.com/zigai/crawlwright/blob/master/LICENSE)
