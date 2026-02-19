# -*- coding: utf-8 -*-
import scrapy
import tldextract
from ARGUS.items import DualCollector
from scrapy.loader import ItemLoader
from scrapy.utils.request import fingerprint
from scrapy.downloadermiddlewares.offsite import OffsiteMiddleware
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.python.failure import Failure
import hashlib
import gzip

import pandas as pd
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from io import BytesIO
import urllib
import urllib.request
from urllib.request import urlopen
from urllib.request import Request
from urllib.parse import urlsplit

from pathlib import Path
import socket
from datetime import datetime


def _loader_first(loader, field, default=None):
    """
    Safely get the first collected value for an ItemLoader field.
    Falls back to default if the field is empty.
    """
    # prefer processed output (if you have processors configured)
    val = loader.get_output_value(field)
    if isinstance(val, (list, tuple)):
        val = val[0] if val else None
    if val is None or val == "":
        vals = loader.get_collected_values(field)
        val = vals[0] if vals else None
    return val if val not in (None, "") else default


class DualSpider(scrapy.Spider):
    name = "dualspider"

    def __init__(
        self,
        url_chunk="",
        limit=50,
        run_id = None,
        ID="BVD",
        url_col="url",
        language="",
        prefer_short_urls="on",
        pdfscrape="off",
        *args,
        **kwargs,
    ):
        super(DualSpider, self).__init__(*args, **kwargs)

        self._agg = {}
        self.run_id = run_id
        self.rows = []
        self.url_chunk = url_chunk
        chunk_path = Path(url_chunk).resolve()
        self.chunk_id = chunk_path.stem.split("_")[-1]
        self.timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        self.hostname = socket.gethostname()

        # Load the CSV chunk
        data = pd.read_csv(
            url_chunk,
            delimiter=",",
            encoding="utf-8",
            on_bad_lines="skip",
            engine="python",
        )
        self.allowed_domains = [
            str(url).split("www.")[-1].lower() for url in list(data[url_col])
        ]
        self.start_urls = ["https://" + url.lower() for url in self.allowed_domains]
        self.IDs = [ID for ID in list(data[ID])]
        self.site_limit = int(limit)
        self.language = language.split("_")
        self.prefer_short_urls = prefer_short_urls
        self.pdfscrape = pdfscrape

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)

        chunk_id = Path(spider.url_chunk).stem.split("_")[-1]
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        crawler.settings.set("WEBARCHIVE_ENABLED", True)

        return spider

    ##################################################################
    # HELPER FUNCTIONS
    ##################################################################

    # filetypes to be filtered
    filetypes = set(
        filetype
        for filetype in [
            "mng",
            "pct",
            "bmp",
            "gif",
            "jpg",
            "jpeg",
            "png",
            "pst",
            "psp",
            "tif",
            "tiff",
            "ai",
            "drw",
            "dxf",
            "eps",
            "ps",
            "svg",
            "mp3",
            "wma",
            "ogg",
            "wav",
            "ra",
            "aac",
            "mid",
            "au",
            "aiff",
            "3gp",
            "asf",
            "asx",
            "avi",
            "mov",
            "mp4",
            "mpg",
            "qt",
            "rm",
            "swf",
            "wmv",
            "m4a",
            "css",
            "doc",
            "exe",
            "bin",
            "rss",
            "zip",
            "rar",
            "msu",
            "flv",
            "dmg",
            "mng?download=true",
            "pct?download=true",
            "bmp?download=true",
            "gif?download=true",
            "jpg?download=true",
            "jpeg?download=true",
            "png?download=true",
            "pst?download=true",
            "psp?download=true",
            "tif?download=true",
            "tiff?download=true",
            "ai?download=true",
            "drw?download=true",
            "dxf?download=true",
            "eps?download=true",
            "ps?download=true",
            "svg?download=true",
            "mp3?download=true",
            "wma?download=true",
            "ogg?download=true",
            "wav?download=true",
            "ra?download=true",
            "aac?download=true",
            "mid?download=true",
            "au?download=true",
            "aiff?download=true",
            "3gp?download=true",
            "asf?download=true",
            "asx?download=true",
            "avi?download=true",
            "mov?download=true",
            "mp4?download=true",
            "mpg?download=true",
            "qt?download=true",
            "rm?download=true",
            "swf?download=true",
            "wmv?download=true",
            "m4a?download=true",
            "css?download=true",
            "doc?download=true",
            "exe?download=true",
            "bin?download=true",
            "rss?download=true",
            "zip?download=true",
            "rar?download=true",
            "msu?download=true",
            "flv?download=true",
            "dmg?download=true",
        ]
    )

    # function to refresh the allowed domain list after adding domains
    def refreshAllowedDomains(self):
        for mw in self.crawler.engine.scraper.spidermw.middlewares:
            if isinstance(mw, OffsiteMiddleware):
                mw.spider_opened(self)

    # function which extracts the subdomain from a url string or response object
    def subdomainGetter(self, response):
        # if string
        if isinstance(response, str):
            tld = tldextract.extract(response)
            if tld.subdomain != "":
                domain = tld.subdomain + "." + tld.registered_domain
                return domain
            else:
                domain = tld.registered_domain
                return domain
        # if scrapy response object
        else:
            tld = tldextract.extract(response.url)
            if tld.subdomain != "":
                domain = tld.subdomain + "." + tld.registered_domain
                return domain
            else:
                domain = tld.registered_domain
                return domain

    def checkRedirectDomain(self, response):
        url_a = response.url
        url_b = response.request.meta.get("orig_slot") or urlsplit(response.url).netloc

        # If url_b is missing, just say "no redirect"
        if not url_a or not url_b:
            return False

        dom_a = tldextract.extract(url_a).registered_domain
        dom_b = tldextract.extract(url_b).registered_domain

        return dom_a != dom_b

    # In your dualspider.py file

    def extractText(self, response):
        # This selector gets all text nodes from within the <body> tag,
        # regardless of how deeply they are nested.
        all_text_parts = response.xpath('//body//text()').getall()

        # Join the parts together and clean up whitespace
        cleaned_text = " ".join(part.strip() for part in all_text_parts if part.strip())

        return cleaned_text

    # function which extracts and returns meta information
    def extractHeader(self, response):
        # Only parse HTML responses
        content_type = response.headers.get('Content-Type', b'').lower()
        if (
            (content_type.startswith(b'text/html') or content_type.startswith(b'application/xhtml+xml'))
            and hasattr(response, "xpath")
        ):
            try:
                title = " ".join(response.xpath("//title/text()").extract())
                description = " ".join(
                    response.xpath("//meta[@name='description']/@content").extract()
                )
                keywords = " ".join(
                    response.xpath("//meta[@name='keywords']/@content").extract()
                )
                language = " ".join(response.xpath("//html/@lang").extract())
            except Exception:
                title = description = keywords = language = ""
        else:
            title = ""
            description = ""
            keywords = ""
            language = ""
        return title, description, keywords, language

    # function which reorders the urlstack, giving highest priority to short urls and language tagged urls

    def reorderUrlstack(self, urlstack, language, prefer_short_urls):
        preferred_language = []
        other_language = []
        language_tags = []
        if language == "None":
            preferred_language = urlstack
        else:
            for ISO in language:
                language_tags.append("/{}/".format(ISO))
                language_tags.append("/{}-{}/".format(ISO, ISO))
                language_tags.append("?lang={}".format(ISO))
            for url in urlstack:
                if any(tag in url for tag in language_tags):
                    preferred_language.append(url)
                else:
                    other_language.append(url)
        if prefer_short_urls == "on":
            urlstack = sorted(preferred_language, key=len) + sorted(
                other_language, key=len
            )
        else:
            urlstack = preferred_language + other_language
        return urlstack

    # function which extracts text of online PDFs
    def pdf_scraping(self, pdf_url):
        text = []

        # solve ASCII url problems
        pdf_url = urllib.parse.quote(pdf_url, safe="://", encoding="UTF-8")

        # access PDF
        # catching errors by specifying user-agent
        remoteFile = urlopen(
            Request(pdf_url, headers={"User-Agent": "Mozilla/5.0"})
        ).read()

        # catch 404 errors
        if urllib.error.HTTPError == 404:
            print("404 ERROR")
            return text

        else:
            memoryFile = BytesIO(remoteFile)

            # read PDF content
            manager = PDFResourceManager()
            retstr = BytesIO()
            layout = LAParams(all_texts=True)
            device = TextConverter(manager, retstr, laparams=layout)
            interpreter = PDFPageInterpreter(manager, device)

            # default: True
            for page in PDFPage.get_pages(memoryFile, check_extractable=False):
                interpreter.process_page(page)

            # do some editing on the pdf text
            extracted_text = (
                retstr.getvalue()
                .decode("utf-8")
                .replace("\n", "")
                .replace("\t", "")
                .replace("\r\n", "")
                .replace("\r", "")
            )

            device.close()
            retstr.close()
            text.append(["pdf", ["[->pdf<-] " + " [->pdf<-] " + str(extracted_text)]])

            # return text to make accessible for loader
            return text

    ##################################################################
    # START REQUEST
    ##################################################################

    # start request and add ID to meta

    def start_requests(self):
        i = -1
        for url in self.start_urls:
            i += 1
            ID = self.IDs[i]
            print(f"Scheduling request for: {url}")
            yield scrapy.Request(
                url,
                callback=self.parse,
                meta={
                    "ID": ID,
                    "orig_slot": urlsplit(url).netloc,
                },
                dont_filter=True,
                # errback=self.errorback,
                errback = self.handle_error,
            )

    def handle_error(self, failure):
        url = getattr(failure.request, "url", "unknown")
        error_msg = repr(failure.value)
        # Log to Scrapy log
        self.logger.error(f"Failed URL: {url} - {error_msg}")
        # Append to failed_urls.txt in project root
        # with open("/home/msalvetti/KOFScraper/failed_urls.txt", "a") as f:
        #     f.write(f"{url}\t{error_msg}\n")
        # Optionally, yield a minimal item for pipeline tracking
        loader = ItemLoader(item=DualCollector())
        loader.add_value("ID", [failure.request.meta.get("ID", "")])
        loader.add_value("dl_slot", [failure.request.meta.get("orig_slot", "")])
        loader.add_value("error", [error_msg])
        loader.add_value("start_page", [url])
        loader.add_value("scraped_urls", [""])
        loader.add_value("html_path", [""])
        loader.add_value("scraped_text", [""])
        loader.add_value("title", [""])
        loader.add_value("description", [""])
        loader.add_value("keywords", [""])
        loader.add_value("language", [""])
        loader.add_value("links", [""])
        loader.add_value("alias", [""])
        yield loader.load_item()

    # errorback creates an collector item, records the error type, and passes it to the pipeline
    def errorback(self, failure):
        loader = ItemLoader(item=DualCollector())
        if failure.check(HttpError):
            response = failure.value.response
            loader.add_value("dl_slot", [response.request.meta.get("download_slot")])
            loader.add_value("start_page", [""])
            loader.add_value("scraped_urls", [""])
            loader.add_value("html_path", [""])
            try:
                loader.add_value("redirect", [self.checkRedirectDomain(response)])
            except Exception:
                loader.add_value("redirect", False)  # fail-safe

            # loader.add_value("redirect", [None])
            loader.add_value("scraped_text", [""])
            loader.add_value("title", [""])
            loader.add_value("description", [""])
            loader.add_value("keywords", [""])
            loader.add_value("language", [""])
            loader.add_value("error", [response.status])
            loader.add_value("ID", [response.request.meta["ID"]])
            loader.add_value("links", [""])
            loader.add_value("alias", [""])
            yield loader.load_item()
        elif failure.check(DNSLookupError):
            request = failure.request
            loader.add_value("dl_slot", [request.meta.get("download_slot")])
            loader.add_value("start_page", [""])
            loader.add_value("scraped_urls", [""])
            loader.add_value("html_path", [""])
            loader.add_value("redirect", [None])
            loader.add_value("scraped_text", [""])
            loader.add_value("title", [""])
            loader.add_value("description", [""])
            loader.add_value("keywords", [""])
            loader.add_value("language", [""])
            loader.add_value("error", ["DNS"])
            loader.add_value("ID", [request.meta["ID"]])
            loader.add_value("links", [""])
            loader.add_value("alias", [""])
            yield loader.load_item()
        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            loader.add_value("dl_slot", request.meta.get("download_slot"))
            loader.add_value("start_page", "")
            loader.add_value("scraped_urls", "")
            loader.add_value("html_path", [""])
            loader.add_value("redirect", [None])
            loader.add_value("scraped_text", "")
            loader.add_value("title", "")
            loader.add_value("description", "")
            loader.add_value("keywords", "")
            loader.add_value("language", "")
            loader.add_value("error", "Timeout")
            loader.add_value("ID", request.meta["ID"])
            loader.add_value("links", "")
            loader.add_value("alias", "")
            yield loader.load_item()
        else:
            request = failure.request
            loader.add_value("dl_slot", request.meta.get("download_slot"))
            loader.add_value("start_page", "")
            loader.add_value("scraped_urls", "")
            loader.add_value("html_path", [""])
            loader.add_value("redirect", [None])
            loader.add_value("scraped_text", "")
            loader.add_value("title", "")
            loader.add_value("description", "")
            loader.add_value("keywords", "")
            loader.add_value("language", "")
            loader.add_value("error", "other")
            loader.add_value("ID", request.meta["ID"])
            loader.add_value("links", "")
            loader.add_value("alias", "")
            yield loader.load_item()

    def _save_raw_html(self, response, ID):
        from pathlib import Path

        run_id = getattr(self, "run_id", self.timestamp)

        raw_dir = Path(self.url_chunk).resolve().parent / f"run_id={run_id}/raw_html"
        raw_dir.mkdir(parents=True, exist_ok=True)

        # deterministic short name by URL hash
        h = hashlib.md5(response.url.encode("utf-8")).hexdigest()[:10]
        fname = f"{ID}_{h}.html.gz"
        fpath = raw_dir / fname

        # write bytes directly (no decode) to a gz file
        with gzip.open(fpath, "wb") as gzf:
            gzf.write(response.body)

        return str(fpath)

    ##################################################################
    # MAIN PARSE
    ##################################################################

    def parse(self, response):

        # initialize collector item which stores the website's content and meta data
        loader = ItemLoader(item=DualCollector())
        html_clean = (
            response.body.decode(getattr(response, "charset", "utf-8"), errors = "replace")
            .replace("\t", "")
            .replace("\n", "")
            .replace("\r", "")
        )
        # loader.add_value("html_raw", html_clean)
        loader.add_value("dl_slot", response.request.meta.get("download_slot"))
        loader.replace_value(
            "dl_slot", response.meta.get("orig_slot") or urlsplit(response.url).netloc
        )

        loader.add_value("redirect", self.checkRedirectDomain(response))
        loader.add_value("start_page", response.url)
        loader.add_value("start_domain", self.subdomainGetter(response))
        loader.add_value("scraped_urls", [response.urljoin(response.url)])
        loader.add_value("scrape_counter", 1)
        loader.add_value("scraped_text", [self.extractText(response)])
        title, description, keywords, language = self.extractHeader(response)
        loader.add_value("title", [title])
        loader.add_value("description", [description])
        loader.add_value("keywords", [keywords])
        loader.add_value("language", [language])
        loader.add_value("error", "None")
        ID = response.request.meta["ID"]
        loader.add_value("ID", [ID])
        html_path = self._save_raw_html(response, ID)
        loader.add_value("html_path", [html_path])
        # add alias if there was an initial redirect
        if self.checkRedirectDomain(response):
            loader.add_value("alias", self.subdomainGetter(response).split("www.")[-1])
        else:
            loader.add_value("alias", "")
        loader.add_value("links", "")

        # initialize the fingerprints set which stores all fingerprints of visited websites
        fingerprints = set()
        # add the fingerprints of the start_page
        fingerprints.add(fingerprint(response.request))

        # if there was an initial redirect, the new domain is added to the allowed domains
        domain = self.subdomainGetter(response)
        if domain not in self.allowed_domains:
            self.allowed_domains.append(domain)
            self.refreshAllowedDomains()

        # extract all urls from the page...
        urls = (
            response.xpath("//a/@href").extract()
            + response.xpath("//frame/@src").extract()
            + response.xpath("//frameset/@src").extract()
        )
        # ...and safe them to a urlstack
        urlstack = [response.urljoin(url) for url in urls]

        response.request.meta.setdefault("agg_key", fingerprint(response.request))
        k = response.request.meta["agg_key"]
        self._agg[k] = {
            "loader" : loader,
            "urlstack" : urlstack,
            "fingerprints" : fingerprints,
        }

        return self.processURLstack(response)

    ##################################################################
    # PROCESS URL STACK
    ##################################################################

    def processURLstack(self, response):
        k = response.request.meta["agg_key"]
        state = self._agg[k]
        loader = state["loader"]
        urlstack = state["urlstack"]
        fingerprints = state["fingerprints"]


        if isinstance(response, Failure):
            self.logger.warning(f"Request failed: {response}")
            return

        # check whether max number of websites has been scraped for this website
        if self.site_limit != 0:
            if loader.get_collected_values("scrape_counter")[0] >= self.site_limit:
                del urlstack[:]

        # reorder the urlstack to scrape the most relevant urls first
        urlstack = self.reorderUrlstack(urlstack, self.language, self.prefer_short_urls)

        # check urlstack for links to other domains
        orig_slot = (
            _loader_first(loader, "dl_slot")
            or response.request.meta.get("download_slot")
            or urlsplit(response.url).netloc
            or response.url
        )  # last resort; response.url should always exist

        alias_slot = _loader_first(loader, "alias")  # may be None

        orig_domain = self.subdomainGetter(orig_slot).split("www.")[-1]
        alias_domain = (
            self.subdomainGetter(alias_slot).split("www.")[-1] if alias_slot else None
        )

        for url in urlstack:
            url = url.replace("\r\n", "").replace("\n", "")
            domain = self.subdomainGetter(url).split("www.")[-1]

            # if url points to domain that is the originally requested domain...
            if domain == orig_domain:
                continue
            # ...and also not the alias domain...
            if alias_domain and domain == alias_domain:
                continue
            # ...add domain to link list
            loader.add_value("links", domain)

        # check if the next url in the urlstack is valid
        while len(urlstack) > 0:
            # pop non-valid domains
            domain = self.subdomainGetter(urlstack[0])
            if domain not in self.allowed_domains:
                urlstack.pop(0)
                continue
            # pop some unwanted urls
            if urlstack[0].startswith("mail"):
                urlstack.pop(0)
                continue
            if urlstack[0].startswith("tel"):
                urlstack.pop(0)
                continue
            if urlstack[0].startswith("javascript"):
                urlstack.pop(0)
                continue
            # pop unwanted filetypes
            if urlstack[0].split(".")[-1].lower() in self.filetypes:
                urlstack.pop(0)
                continue
            if self.pdfscrape == "off":
                if urlstack[0].split(".")[-1].lower() == "pdf":
                    urlstack.pop(0)
                    continue
            # pop visited urls.
            # also pop urls that cannot be requested
            # (potential bottleneck: Request has to be sent to generate fingerprint from)
            if fingerprint(scrapy.Request(urlstack[0], callback=None)) in fingerprints:
                urlstack.pop(0)
            else:
                break
        # if the url was assessed to be valid, send out a request and callback the parse_subpage function
        # errbacks return to processURLstack
        # ALLOW ALL HTTP STATUS:
        # errors must be caught in the callback function, because middleware caught request break the sequence and collector items get lost
        if len(urlstack) > 0:

            # if url is actually a pdf file
            if str(urlstack[0])[-4:] == ".pdf":

                # access url that has been recognized as pdf
                pdf_url = urlstack.pop(0)

                loader.replace_value(
                    "scrape_counter",
                    loader.get_collected_values("scrape_counter")[0] + 1,
                )
                loader.add_value("scraped_urls", pdf_url)
                try:
                    # add pdf text to loader
                    loader.add_value("scraped_text", [self.pdf_scraping(pdf_url)])
                except:
                    loader.add_value("scraped_text", "")
                # loader.add_value("scraped_text", [self.pdf_scraping(pdf_url)])  # add pdf text to loader
                loader.add_value("title", "")
                loader.add_value("description", "")
                loader.add_value("keywords", "")
                loader.add_value("language", "")

                # check if urlstack is empty after popping pdf
                if len(urlstack) == 0:
                    return self.processURLstack(response)

                else:
                    # check if the next url in the urlstack is valid
                    while len(urlstack) > 0:
                        # pop non-valid domains
                        domain = self.subdomainGetter(urlstack[0])
                        if domain not in self.allowed_domains:
                            urlstack.pop(0)
                            continue
                        # pop some unwanted urls
                        if urlstack[0].startswith("mail"):
                            urlstack.pop(0)
                            continue
                        if urlstack[0].startswith("tel"):
                            urlstack.pop(0)
                            continue
                        if urlstack[0].startswith("javascript"):
                            urlstack.pop(0)
                            continue
                        # pop unwanted filetypes
                        if urlstack[0].split(".")[-1].lower() in self.filetypes:
                            urlstack.pop(0)
                            continue
                        # pop visited urls.
                        # also pop urls that cannot be requested
                        # (potential bottleneck: Request has to be sent to generate fingerprint from)
                        if (
                            fingerprint(scrapy.Request(urlstack[0], callback=None))
                            in fingerprints
                        ):
                            urlstack.pop(0)
                        else:
                            break

                    # check if urlstack is emtpy after previous checks
                    if len(urlstack) == 0:
                        return self.processURLstack(response)

                    else:
                        # make next request
                        yield scrapy.Request(
                            urlstack.pop(0),
                            meta={
                                "handle_httpstatus_all": True,
                                **response.meta,
                            },
                            dont_filter=True,
                            callback=self.parse_subpage,
                            errback=self.processURLstack,
                        )

            else:
                yield scrapy.Request(
                    urlstack.pop(0),
                    meta={
                        "handle_httpstatus_all": True,
                        **response.meta,
                    },
                    dont_filter=True,
                    callback=self.parse_subpage,
                    errback=self.processURLstack,
                )

        # if there are no urls left in the urlstack, the website was scraped completely and the item can be sent to the pipeline
        else:
            yield loader.load_item()
            del self._agg[k]

    ##################################################################
    # PARSE SUB PAGE
    ##################################################################

    def parse_subpage(self, response):
        k = response.request.meta["agg_key"]
        state = self._agg[k]

        loader = state["loader"]

        if fingerprint(response.request) in state["fingerprints"]:
            return self.processURLstack(response)
        state["fingerprints"].add(fingerprint(response.request))

        # try to catch some errors
        try:
            # if http client errors
            if response.status > 308:
                return self.processURLstack(response)

            # if redirect sent us to an non-allowed domain
            elif self.subdomainGetter(response) not in self.allowed_domains:
                return self.processURLstack(response)

            # skip broken urls
            if response.status == 301:
                # revive the loader from the response meta data
                # loader = response.meta["loader"]
                # k = response.request.meta["agg_key"]
                # loader = self._agg[k]["loader"]

                # check whether this request was redirected to a allowed url which is actually another firm
                if loader.get_collected_values("start_domain")[
                    0
                ] != self.subdomainGetter(response):
                    raise ValueError()

                # extract urls and add them to the urlstack
                urls = (
                    response.xpath("//a/@href").extract()
                    + response.xpath("//frame/@src").extract()
                    + response.xpath("//frameset/@src").extract()
                )
                for url in urls:
                    # response.meta["urlstack"].append(response.urljoin(url))
                    # k = response.request.meta["agg_key"]
                    # self._agg[k]["urlstack"].append(response.urljoin(url))
                    state["urlstack"].append(response.urljoin(url))

                # pass back the updated urlstack
                return self.processURLstack(response)

            if response.status == 302:
                # revive the loader from the response meta data
                # loader = response.meta["loader"]
                # k = response.request.meta["agg_key"]
                # loader = self._agg[k]["loader"]

                # check whether this request was redirected to a allowed url which is actually another firm
                if loader.get_collected_values("start_domain")[
                    0
                ] != self.subdomainGetter(response):
                    raise ValueError()

                # extract urls and add them to the urlstack
                urls = (
                    response.xpath("//a/@href").extract()
                    + response.xpath("//frame/@src").extract()
                    + response.xpath("//frameset/@src").extract()
                )
                for url in urls:
                    # response.meta["urlstack"].append(response.urljoin(url))
                    # k = response.request.meta["agg_key"]
                    state["urlstack"].append(response.urljoin(url))
                    # self._agg[k]["urlstack"].append(response.urljoin(url))

                # pass back the updated urlstack
                return self.processURLstack(response)

            # if everything is cool, do the normal stuff
            else:
                # revive the loader from the response meta data
                # loader = response.meta["loader"]
                # k = response.request.meta["agg_key"]
                # loader = self._agg[k]["loader"]

                ID = (
                    loader.get_collected_values("ID")[0]
                    if loader.get_collected_values("ID")
                    else response.meta["ID"]
                )
                html_path = self._save_raw_html(response, ID)
                loader.add_value("html_path", [html_path])

                # check whether this request was redirected to an allowed url which is actually another firm
                if loader.get_collected_values("start_domain")[
                    0
                ] != self.subdomainGetter(response):
                    raise ValueError()

                # extract urls and add them to the urlstack
                urls = (
                    response.xpath("//a/@href").extract()
                    + response.xpath("//frame/@src").extract()
                    + response.xpath("//frameset/@src").extract()
                )
                for url in urls:
                    # response.meta["urlstack"].append(response.urljoin(url))
                    # k = response.request.meta["agg_key"]
                    # self._agg[k]["urlstack"].append(response.urljoin(url))
                    state["urlstack"].append(response.urljoin(url))


                # add info to collector item
                loader.replace_value(
                    "scrape_counter",
                    loader.get_collected_values("scrape_counter")[0] + 1,
                )
                loader.add_value("scraped_urls", [response.urljoin(response.url)])
                loader.add_value("scraped_text", [self.extractText(response)])
                title, description, keywords, language = self.extractHeader(response)
                loader.add_value("title", [title])
                loader.add_value("description", [description])
                loader.add_value("keywords", [keywords])
                loader.add_value("language", [language])

                # pass back the updated urlstack
                return self.processURLstack(response)

        # in case of errors, opt out and fall back to processURLstack
        # except:
        #     return self.processURLstack(response)
        # NEW CODE (Reveals the error)
        except Exception as e:
            self.logger.error(f"An error occurred while parsing {response.url}: {e}")
            return self.processURLstack(response)
