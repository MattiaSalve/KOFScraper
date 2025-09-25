from pathlib import Path
from scrapy.exporters import CsvItemExporter
from ARGUS.items import DualExporter
from bin.durations import save_spider_duration
import pandas as pd

import time

# import datetime
from datetime import datetime
import re
import gzip

import pyarrow as pa
import pyarrow.parquet as pq



class DualPipeline(object):
    def open_spider(self, spider):
        url_chunk_path = Path(spider.url_chunk).resolve()
        chunk_id = url_chunk_path.stem.split("_")[-1]
        run_id = datetime.now().strftime("%d.%m.%Y")
        spider.run_id = run_id

        output_dir = url_chunk_path.parent
        output_dir_light = url_chunk_path.parent / f"run_id={run_id}/parsed"
        output_dir_light.mkdir(parents=True, exist_ok=True)

        self._parquet_path = output_dir_light / f"ARGUS_chunk_{chunk_id}.parquet"
        self._rows = []  # collect rows as dicts
        self._light_rows = 0

        # --- LIGHT CSV ---
        # light_path = output_dir_light / f"ARGUS_chunk_{chunk_id}.csv.gz"
        # self.f = gzip.open(light_path, mode="wb")
        # self.exporter = CsvItemExporter(
        #     self.f, encoding="utf-8", delimiter="\t", include_headers_line=True
        # )
        # self.exporter.fields_to_export = [
        #     "ID",
        #     "dl_rank",
        #     "dl_slot",
        #     "alias",
        #     "error",
        #     "redirect",
        #     "start_page",
        #     "title",
        #     "keywords",
        #     "description",
        #     "language",
        #     "text",
        #     "links",
        #     "timestamp",
        #     "url",
        #     "html_path",
        # ]
        # self.exporter.start_exporting()

        # --- RAW CSV ---

        self._chunk_id = chunk_id
        self._output_dir = output_dir
        self._tag_pattern = re.compile(r"(\[->.+?<-\] ?)+?")

        # (optional) simple counters
        self._t0 = time.perf_counter()

    def close_spider(self, spider):
        # 1) Close exporters first (flush)
        try:
            table = pa.Table.from_pylist(self._rows)
            pq.write_table(table, self._parquet_path, compression='zstd')
            spider.logger.info('Wrote %d rows to %s', len(self._rows), self._parquet_path)
        except Exception as e:
            spider.logger.error("Error closing exporters: %s", e, exc_info=True)

        try:
            stats = spider.crawler.stats

            # Try Scrapy's own value first
            elapsed = stats.get_value("elapsed_time_seconds")

            if elapsed is None:
                start_time = stats.get_value("start_time")
                if start_time:
                    # compute using start_time and 'now'
                    now = datetime.now(tz=getattr(start_time, "tzinfo", None))
                    elapsed = (now - start_time).total_seconds()
                    spider.logger.info(
                        "Computed elapsed from start_time: %.3fs", elapsed
                    )
                elif hasattr(self, "_t0"):
                    elapsed = time.perf_counter() - self._t0
                    spider.logger.info(
                        "Computed elapsed from perf_counter: %.3fs", elapsed
                    )
                else:
                    spider.logger.warning(
                        "Could not determine duration; skipping write"
                    )
                    return

            # Persist

            name = getattr(self, "_chunk_id", spider.name)
            url_chunk_path = Path(spider.url_chunk).resolve()
            output_dir_light = url_chunk_path.parent / f"run_id={spider.run_id}/parsed"

            save_spider_duration(
                f"spider_{name}",
                float(elapsed),
                output_dir_light / "spider_durations.csv",
            )
            spider.logger.info("Saved duration for %s: %.3fs", name, elapsed)

        except Exception as e:
            spider.logger.error("Failed to save duration: %s", e, exc_info=True)

    def process_item(self, item, spider):
        # if self._light_rows % 100 == 0:
        #     self.f.flush()

        scraped_text = item["scraped_text"]

        for c, webpage_text in enumerate(item["scraped_text"]):
            url = item["scraped_urls"][c]
            timestamp = datetime.fromtimestamp(time.time()).strftime("%c")

            # -------- LIGHT ROW --------
            row = DualExporter()
            row["ID"] = item["ID"][0]
            row["dl_rank"] = c
            row["dl_slot"] = (item.get("dl_slot") or [None])[0]
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
            row["html_path"] = item.get("html_path", [""])[c]

            tag_pattern = self._tag_pattern
            # webpage_text = ""
            # for tagchunk in webpage:
            #     text_piece = " ".join(tagchunk[-1][0].split()).strip()
            #     if not text_piece:
            #         continue
            #     parts = re.split(tag_pattern, text_piece)
            #     acc = ""
            #     for i, elem in enumerate(parts):
            #         if i % 2 == 0 and elem.strip().strip('"'):
            #             acc += parts[i - 1] + elem
            #     if acc:
            #         webpage_text += ". " + acc
            #
            # row["text"] = (
            # webpage_text[2:] if webpage_text.startswith(". ") else webpage_text
            # )
            row["text"] = webpage_text
            # self.exporter.export_item(row)
            self._rows.append(dict(row))
            self._light_rows += 1

        return item
