import abc
import scrapy
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler


class BaseSpider(scrapy.Spider, abc.ABC):

    def _set_crawler(self, crawler):
        super()._set_crawler(crawler)
        if self.settings.get('LOG_ENABLED', False):
            log_file_path = f"{self.settings.get('LOG_FILE_PATH')}" \
                            f"{self.name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.log"

            logger = logging.getLogger()
            logger.setLevel(self.settings.get('LOG_LEVEL'))
            formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s',
                                          '%Y-%m-%d %H:%M:%S')
            fhlr = RotatingFileHandler(log_file_path,
                                       maxBytes=50 * 1024 * 1024,
                                       backupCount=5,
                                       encoding='utf-8-sig')
            fhlr.setLevel(self.settings.get('LOG_LEVEL'))
            fhlr.setFormatter(formatter)
            logger.addHandler(fhlr)
