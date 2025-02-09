# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

from itemadapter import ItemAdapter
from pymongo import MongoClient
from scrapy.exceptions import DropItem
import logging



class ChitaiGorodParserPipeline:

    def __init__(self, mongo_uri, mongo_db, mongo_collection_name):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection_name = mongo_collection_name

    @classmethod
    def from_crawler(cls, crawler):
        mongo_db = crawler.settings.get("MONGO_DB", "books")
        mongo_user = crawler.settings.get("MONGO_USER")
        mongo_pass = crawler.settings.get("MONGO_PASS")
        mongo_port = crawler.settings.get("MONGO_PORT", 27017)
        mongo_collection_name = crawler.settings.get("MONGO_COLLECTION_NAME", "item")
        mongo_url = f"mongodb://{mongo_user}:{mongo_pass}@localhost:{mongo_port}"
        return cls(
            mongo_uri=mongo_url,
            mongo_db=mongo_db,
            mongo_collection_name = mongo_collection_name  
        )

    def open_spider(self, spider):
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        logging.info(f"Processing item: {item['title']}")
        valid = True
        for data in item:
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))
        if valid:
            self.db[self.mongo_collection_name].insert_one(dict(item))
            spider.logger.info("Book added to MongoDB")
        return item