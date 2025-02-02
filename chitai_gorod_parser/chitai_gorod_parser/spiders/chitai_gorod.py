import scrapy


class ChitaiGorodSpider(scrapy.Spider):
    name = "chitai_gorod"
    allowed_domains = ["chitai-gorod.ru"]
    start_urls = ["https://chitai-gorod.ru"]

    def parse(self, response):
        pass
