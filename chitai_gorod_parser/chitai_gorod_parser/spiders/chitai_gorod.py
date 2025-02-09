import scrapy

from scrapy.spiders import SitemapSpider
from chitai_gorod_parser.items import ChitaiGorodParserItem
from chitai_gorod_parser.pipelines import ChitaiGorodParserPipeline


class ChitaiGorodSpider(SitemapSpider):
    name = "books"
    sitemap_urls = ['https://www.chitai-gorod.ru/sitemap.xml']
    sitemap_follow = ['authors']
    sitemap_rules = [
        ('/author/', 'parse_author')
    ]

    custom_settings = {
        "ITEM_PIPELINES": {"chitai_gorod_parser.pipelines.ChitaiGorodParserPipeline" : 100},
        "MONGO_DB" : "scrape_catalog",
        "MONGO_USER" : "admin",
        "MONGO_PASS" : "admin123",
        "MONGO_COLLECTION_NAME": "books"

    }
    def parse_author(self, response):
        author_name = response.xpath('//h1/text()').get()
        books = response.xpath('//div[@class = "product-card__text product-card__row"]/a[@class="product-card__title"]/@href').getall()
        for book in books:
            self.logger.info(f"Found book URL: {response.urljoin(book)}")
            full_book_url = response.urljoin(book)  # Собираем полный URL
            self.logger.info(f"Found book URL: {full_book_url}")

            yield response.follow(
                    full_book_url,
                    callback=self.parse_book,
                    cb_kwargs={'author_name': author_name})

        next_page_url = response.xpath('//a[contains(@class, "pagination__button--icon")]/@href').get()
        if next_page_url:
            yield response.follow(next_page_url, callback=self.parse_author)

    def parse_book(self, response, author_name):
        item = ChitaiGorodParserItem()
        self.logger.info(f"hereeee!! {author_name}")
        item['title'] = response.xpath('//h1[@class="detail-product__header-title"]/text()').get()
        item['author'] = author_name
        item['description'] = self.stripper(response.xpath('//div[@class="product-description-short__text"]/text()').get())
        price_count = response.xpath('//span[@itemprop="price"]/@content').get()
        item['price_amount'] = price_count if price_count else response.xpath('//*[@itemprop="offers"]/@content').get()
        item['price_currency'] = response.xpath('//meta[@itemprop="priceCurrency"]/@content').get()
        item['rating_value'] = response.xpath('//meta[@itemprop="ratingValue"]/@content').get()
        item['rating_count'] = response.xpath('//meta[@itemprop="reviewCount"]/@content').get()
        item['publication_year'] = self.stripper(response.xpath('//span[@itemprop="datePublished"]/text()').get())
        item['isbn'] = self.stripper(response.xpath('//span[@itemprop="isbn"]/text()').get())
        item['pages_cnt'] = self.stripper(response.xpath('//span[@itemprop="numberOfPages"]/text()').get())
        item['publisher'] = self.stripper(response.xpath('//a[@itemprop="publisher"]/text()').get())
        item['book_cover'] = response.xpath('//img[@class="product-info-gallery__poster"]/@src').get()
        item['source_url'] = response.url

        self.logger.info(f"{item}")
        if  item['title'] and item['publication_year'] and item['isbn'] and item['pages_cnt'] and item['source_url']:
            yield item

    def stripper(self, string):
        return None if not string else string.strip()
