# Scrapy settings for SAP partners project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'techpartners'

SPIDER_MODULES = ['techpartners.spiders']
NEWSPIDER_MODULE = 'techpartners.spiders'


URLLENGTH_LIMIT = 50000

# Obey robots.txt rules
ROBOTSTXT_OBEY = False
HTTPERROR_ALLOW_ALL = True

# DOWNLOAD_TIMEOUT = 120
# DNS_TIMEOUT=30
# RETRY_TIMES = 3
# Configure maximum concurrent requests performed by Scrapy (default: 16)
# CONCURRENT_REQUESTS = 50

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 0.5
# The download delay setting will honor only one of:
# CONCURRENT_REQUESTS_PER_DOMAIN = 1
# CONCURRENT_REQUESTS_PER_IP = 1

# Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
    # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    # 'Accept-Language': 'en-US,en;q=0.5',
    # 'content-type': 'text/plain',
    'Connection': 'keep-alive',
    'TE': 'trailers',
}

#DOWNLOADER_MIDDLEWARES = {
    # ...
    # 'scrapy.downloadermiddlewares.defaultheaders.DefaultHeadersMiddleware': 100,
    # 'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
    # 'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
    # ...
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
   'techpartners.pipelines.SapPipeline': 300,
}

LOG_LEVEL = 'INFO'
LOG_FILE_PATH = './logs/'
LOG_ENABLED = True
DATA_FILE_PATH = './output/'