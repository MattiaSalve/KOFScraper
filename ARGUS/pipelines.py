from pathlib import Path
from scrapy.exporters import CsvItemExporter
from ARGUS.items import DualExporter
import ARGUS.data as data
from bin.durations import save_spider_duration

import time
import datetime
import re
import gzip


class DualPipeline(object):
    def open_spider(self, spider):
        url_chunk_path = Path(spider.url_chunk).resolve()
        chunk_id = url_chunk_path.stem.split("_")[-1]

        output_dir = url_chunk_path.parent
        output_dir.mkdir(parents=True, exist_ok=True)

        # --- LIGHT CSV ---
        light_path = output_dir / f"ARGUS_chunk_{chunk_id}.csv.gz"
        self.f = gzip.open(light_path, mode="wb")
        self.exporter = CsvItemExporter(
            self.f, encoding="utf-8", delimiter="\t", include_headers_line=True
        )
        self.exporter.fields_to_export = [
            "ID",
            "dl_rank",
            "dl_slot",
            "alias",
            "error",
            "redirect",
            "start_page",
            "title",
            "keywords",
            "description",
            "language",
            "text",
            "links",
            "timestamp",
            "url",
        ]
        self.exporter.start_exporting()

        # --- RAW CSV ---
        raw_path = output_dir / f"ARGUS_chunk_{chunk_id}_raw.csv.gz"
        self.raw_f = gzip.open(raw_path, mode="wb")
        self.raw_exporter = CsvItemExporter(
            self.raw_f, encoding="utf-8", delimiter="\t", include_headers_line=True
        )
        self.raw_exporter.fields_to_export = [
            "ID",
            "dl_rank",
            "url",
            "timestamp",
            "html_raw",
        ]
        self.raw_exporter.start_exporting()

        self._chunk_id = chunk_id
        self._output_dir = output_dir
        self._tag_pattern = re.compile(r"(\[->.+?<-\] ?)+?")

        # (optional) simple counters
        self._raw_rows = 0
        self._light_rows = 0

    def close_spider(self, spider):
        # 1) finish & close files FIRST so buffers flush
        if hasattr(self, "exporter"):
            self.exporter.finish_exporting()
        if hasattr(self, "raw_exporter"):
            self.raw_exporter.finish_exporting()
        if hasattr(self, "f"):
            self.f.close()
        if hasattr(self, "raw_f"):
            self.raw_f.close()

        # 2) now safe to do post-run tasks
        try:
            stats = self.crawler.stats
            start_time = stats.get_value("start_time")
            end_time = stats.get_value("finish_time")
            duration = (end_time - start_time).total_seconds()
            print(f"Spider duration: {duration} seconds")
            save_spider_duration(f"spider_{self._chunk_id}", duration)

            print(f"[LIGHT rows written] {self._light_rows}")
            print(f"[RAW rows written]   {self._raw_rows}")

            # quick sanity read (optional)
            # light_path = self._output_dir / f"ARGUS_chunk_{self._chunk_id}.csv.gz"
            # _ = data.load_data(light_path)

        except Exception as e:
            print(f"[ERROR] Post-analysis failed: {e}")

    def process_item(self, item, spider):
        scraped_text = item["scraped_text"]

        for c, webpage in enumerate(scraped_text):
            # define url & timestamp ONCE per row
            url = item["scraped_urls"][c]
            timestamp = datetime.datetime.fromtimestamp(time.time()).strftime("%c")

            # -------- LIGHT ROW --------
            row = DualExporter()
            row["ID"] = item["ID"][0]
            row["dl_rank"] = c
            row["dl_slot"] = item["dl_slot"][0]
            row["alias"] = item["alias"][0]
            row["error"] = item["error"]
            row["redirect"] = item["redirect"][0]
            row["start_page"] = item["start_page"][0]
            row["url"] = url
            row["timestamp"] = timestamp

            row["title"] = item["title"][c].strip()
            row["description"] = item["description"][c].strip()
            row["keywords"] = item["keywords"][c].strip()
            row["language"] = item["language"][c].strip()
            row["links"] = list({link for link in item["links"] if link})

            tag_pattern = self._tag_pattern
            webpage_text = ""
            for tagchunk in webpage:
                text_piece = " ".join(tagchunk[-1][0].split()).strip()
                if not text_piece:
                    continue
                parts = re.split(tag_pattern, text_piece)
                acc = ""
                for i, elem in enumerate(parts):
                    if i % 2 == 0 and elem.strip().strip('"'):
                        acc += parts[i - 1] + elem
                if acc:
                    webpage_text += ". " + acc

            row["text"] = (
                webpage_text[2:] if webpage_text.startswith(". ") else webpage_text
            )
            self.exporter.export_item(row)
            self._light_rows += 1

            # -------- RAW ROW --------
            raw_record = {
                "ID": item["ID"][0],
                "dl_rank": c,
                "url": url,
                "timestamp": timestamp,
                "html_raw": item.get("html_raw", [""])[0],
            }
            self.raw_exporter.export_item(raw_record)  # <-- correct exporter
            self._raw_rows += 1

        return item
