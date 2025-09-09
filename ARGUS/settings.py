import os

BOT_NAME = "ARGUS"

SPIDER_MODULES = ["ARGUS.spiders"]
NEWSPIDER_MODULE = "ARGUS.spiders"

# Obey robots.txt rules
ROBOTSTXT_OBEY = True
HTTPCACHE_ENABLED = True
HTTPCACHE_POLICY = "scrapy.extensions.httpcache.DummyPolicy"

# Concurrency and throttling
CONCURRENT_REQUESTS = 16
REACTOR_THREADPOOL_MAXSIZE = 30
DOWNLOAD_DELAY = 0.5
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 60
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0

# Timeout and retry
DOWNLOAD_TIMEOUT = 30
CONCURRENT_REQUESTS_PER_DOMAIN = 4
RETRY_TIMES = 2
RETRY_ENABLED = True
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 408]

# Request and response handling
# DEFAULT_REQUEST_HEADERS = {
#     "User-Agent": "Mozilla/5.0 ... Chrome/124 Safari/537.36",
#     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#     "Accept-Language": "en-US,en;q=0.9,de-CH;q=0.8,fr-CH;q=0.8",
#     "Connection": "keep-alive",
# }
# COOKIES_ENABLED = True
COMPRESSION_ENABLED = False
DNSCACHE_ENABLED = True
DNS_TIMEOUT = 10
DOWNLOAD_MAXSIZE = 10_000_000
DNS_RESOLVER = "scrapy.resolver.CachingHostnameResolver"
DOWNLOADER_CLIENT_TLS_METHOD = "TLSv1.2"

# Logging
LOG_LEVEL = "INFO"

DOWNLOADER_MIDDLEWARES = {
    # "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    # "scrapy_fake_useragent.middleware.RandomUserAgentMiddleware": 400,
    # "scrapy_webarchive.downloadermiddlewares.WaczMiddleware": 450,
    # "scrapy.downloadermiddlewares.redirect.RedirectMiddleware": 600,
}
USER_AGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36"

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
