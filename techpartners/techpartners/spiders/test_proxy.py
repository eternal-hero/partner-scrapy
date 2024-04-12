from techpartners.spiders.base_spider import BaseSpider

class TestSpider(BaseSpider):
	name = 'test'
	start_urls = ['http://ident.me']
	custom_settings = {
		# 'DOWNLOAD_DELAY': 0.5,
		'CONCURRENT_REQUESTS': 50,
		'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
		'CONCURRENT_REQUESTS_PER_IP': 1,
		'DOWNLOADER_MIDDLEWARES': {
			'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
			'rotating_proxies.middlewares.BanDetectionMiddleware': 620},
		'ROTATING_PROXY_LIST_PATH': 'proxy_25000.txt',
		'ROTATING_PROXY_PAGE_RETRY_TIMES': 10,
	}

	def parse(self, response):
		print("*" * 50)
		print("RESPONSE: " + response.text)
		print("*" * 50)

