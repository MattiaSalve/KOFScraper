BOT_NAME = "ARGUS"

SPIDER_MODULES = ["ARGUS.spiders"]
NEWSPIDER_MODULE = "ARGUS.spiders"

FEED_EXPORT_BATCH_ITEM_COUNT = 1000  # flush after every 1000 items

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Concurrency and throttling
CONCURRENT_REQUESTS = 8
REACTOR_THREADPOOL_MAXSIZE = 30
DOWNLOAD_DELAY = 1
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 2
AUTOTHROTTLE_MAX_DELAY = 60
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0

# Timeout and retry
DOWNLOAD_TIMEOUT = 20
CONCURRENT_REQUESTS_PER_DOMAIN = 4
RETRY_TIMES = 5
RETRY_ENABLED = True
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 408]

# ROTATING_PROXY_LIST_PATH = '/home/msalvetti/ProxyGather/working-proxies-2025-10-16_13-44-14-http.txt'
# ROTATING_PROXY_PAGE_RETRY_TIMES = 5
# ROTATING_PROXY_BAN_POLICY = 'rotating_proxies.policy.BanDetectionPolicy'


# DEFAULT_REQUEST_HEADERS = {
#     "User-Agent": "Mozilla/5.0 ... Chrome/124 Safari/537.36", 
#     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#     "Accept-Language": "en-US,en;q=0.9,de-CH;q=0.8,fr-CH;q=0.8",
#     "Connection": "keep-alive",
# }
COOKIES_ENABLED = True

COMPRESSION_ENABLED = False
DNSCACHE_ENABLED = True
DNS_TIMEOUT = 10
DOWNLOAD_MAXSIZE = 10_000_000_000 
DNS_RESOLVER = "scrapy.resolver.CachingHostnameResolver"
DOWNLOADER_CLIENT_TLS_METHOD = "TLSv1.2"

# Logging
LOG_LEVEL = "INFO"

DOWNLOADER_MIDDLEWARES = {
    'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
    'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
    'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
}


# USER_AGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36"

EXTENSIONS = {"extensions.stats_dump.StatsDump": 500}

# Pipelines (yours)
ITEM_PIPELINES = {
    "ARGUS.pipelines.DualPipeline": 1,
}

# Twisted settings
TWISTED_REACTOR = "twisted.internet.selectreactor.SelectReactor"

# AJAX crawling
AJAXCRAWL_ENABLED = True

# Request Fingerprinter (for compatibility)
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"

PARQUET_RUN_ID_FORMAT = "%d.%m.%Y"
PARQUET_RUN_ID = None
