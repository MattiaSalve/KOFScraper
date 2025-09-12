# extensions/stats_dump.py
from scrapy import signals
import json, logging


class StatsDump:
    @classmethod
    def from_crawler(cls, crawler):
        ext = cls()
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    def spider_closed(self, spider, reason):
        s = spider.crawler.stats.get_stats()
        keys = [
            k
            for k in s.keys()
            if any(
                t in k
                for t in (
                    "response_status_count",
                    "retry",
                    "exception",
                    "downloader/exception_type_count",
                    "httpcompression",
                    "log_count/ERROR",
                    "start_time",
                    "finish_time",
                )
            )
        ]
        logging.getLogger(__name__).warning(
            "STATS:\n%s",
            json.dumps({k: s[k] for k in sorted(keys)}, indent=2, default=str),
        )
