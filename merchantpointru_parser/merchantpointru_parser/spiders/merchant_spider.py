import scrapy

from merchantpointru_parser.items import MerchantpointruParserItem

class MerchantpointruSpider(scrapy.Spider):
    name = "merchant_spider"
    allowed_domain = ["merchantpoint.ru"]
    start_urls = ['https://merchantpoint.ru/brands/63']

    def parse(self, response):
        brands_url = response.xpath('//a[contains(text(), "Бренды")]/@href').get()
        if brands_url:
            yield response.follow(brands_url, callback=self.parse_brands_tab)
        else:
            self.logger.warning("Ссылка на бренды не найдена")

    def parse_brands_tab(self, response):

        brand_links = response.xpath('//tbody/tr/td[2]/a/@href').getall()
        
        next_page_url = response.xpath('//a[@class="page-link"][contains(text(),"Вперед")]/@href').get()
       
        for link in brand_links:
            yield response.follow(link, callback=self.parse_organization_tab)    
        if next_page_url:
            yield response.follow(next_page_url, callback = self.parse_brands_tab)    

    def parse_organization_tab(self, response):
        org_name = response.xpath('//*[@id="layout-content"]/div/div[1]/div/div/h1/text()').get()
        org_description = response.xpath('//*[@id="home"]/div/div/div[1]/div/div[2]/div/p[2]/text()').get()
        points = response.xpath('//*[@id="terminals"]/div/div/div/div/div/div//table//a/@href').getall()
        self.logger.info(f"parse_organization {points}")
        for point in points:
              yield response.follow(point, callback=self.parse_trade_points, meta={
                'org_name': org_name,
                'org_description': org_description
            })




    def parse_trade_points(self, response):
        org_name = response.meta['org_name']
        org_description = response.meta['org_description']


        self.logger.info(f"parse_trade_points")


        item = MerchantpointruParserItem()
        item['merchant_name'] = response.xpath('//*[@id="home"]/div/div/div[1]/div/div[2]/p[2]/text()').get()
        item['mcc'] = response.xpath('//*[@id="home"]/div/div/div[1]/div/div[2]/p[4]/a/text()').get()
        item['address'] = response.xpath('//*[@id="home"]/div/div/div[1]/div/div[2]/p[7]/text()').get()
        item['geo_coordinates'] = response.xpath('//*[@id="home"]/div/div/div[1]/div/div[2]/p[8]/text()').get()
        item['org_name'] = org_name
        item['org_description'] = org_description
        item['source_url'] = response.url

        self.logger.info(f"{item['merchant_name']}")
        if item['merchant_name'] and item['mcc'] and item['org_name'] and item['org_description'] and item['source_url']:
            yield item