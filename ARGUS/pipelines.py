from pathlib import Path
from ARGUS.items import DualExporter
from bin.durations import save_spider_duration
# filelock and os are no longer needed, as the compaction logic was removed.

import time
from datetime import datetime
import re
import json

import pyarrow as pa
import pyarrow.parquet as pq

class DualPipeline(object):
    _BATCH_SIZE = 2000

    # RUN_ID is set in open_spider from spider attributes
    # RUN_ID = datetime.now().strftime("%d.%m.%Y")
    
    def open_spider(self, spider):
        url_chunk_path = Path(spider.url_chunk).resolve()
        chunk_id = url_chunk_path.stem.split("_")[-1]

        run_id = getattr(spider, "run_id", "default_run")

        output_dir = url_chunk_path.parent
        out_dir = output_dir / f"run_id={run_id}/parsed"
        out_dir.mkdir(parents=True, exist_ok=True)

        self._quarantine_path = out_dir / f"ARGUS_{spider.name}_chunk_{chunk_id}.quarantine.jsonl"
        
        # Removed self._parts_dir, as it was unused.

        # This is the single, final output file.
        self._parquet_path = out_dir / f"ARGUS_chunk_{chunk_id}.parquet"
        self._duration_path = out_dir / 'spider_durations.csv'

        # Explicit, stable schema (stringy on purpose; only dl_rank is int)
        self._schema = pa.schema(
            [
                ("ID", pa.string()),
                ("dl_rank", pa.int64()),
                ("dl_slot", pa.string()),
                ("alias", pa.string()),
                ("error", pa.string()),
                ("redirect", pa.string()),
                ("start_page", pa.string()),
                ("url", pa.string()),
                ("timestamp", pa.string()),
                ("title", pa.string()),
                ("description", pa.string()),
                ("keywords", pa.string()),
                ("language", pa.string()),
                ("links", pa.string()),
                ("html_path", pa.string()),
                ("text", pa.string()),
            ]
        )

        self._rows = []
        self._part_seq = 0
        self._writer = None
        self._tag_pattern = re.compile(r"<.*?>|&(?:[a-z0-9]+|#\d+);")
        self._t0 = time.perf_counter()

        self._written_count = 0
        self._dropped_count = 0

        spider.logger.info("Parquet target: %s", self._parquet_path)

    # ---------- helpers
    _STRING_COLS = {
        "ID",
        "dl_slot",
        "alias",
        "error",
        "redirect",
        "start_page",
        "url",
        "timestamp",
        "title",
        "description",
        "keywords",
        "language",
        "html_path",
        "text",
        "links",
    }

    def _coerce_str(self, v):
        if v is None:
            return None
        if isinstance(v, (bytes, bytearray)):
            try:
                return v.decode("utf-8", "replace")
            except Exception:
                return v.decode("latin-1", "replace")
        if isinstance(v, (list, tuple, set, dict)):
            import json

            return json.dumps(v, ensure_ascii=False)  # <- ALWAYS a string now
        return str(v)

    def _sanitize_row(self, row_dict: dict) -> dict:
        r = dict(row_dict)
        r["dl_rank"] = int(r.get("dl_rank") or 0)
        for k in self._STRING_COLS:
            r[k] = self._coerce_str(r.get(k))
        return r

    def _safe_idx(self, seq, i, default=""):
        try:
            if seq is None:
                return default
            return seq[i]
        except Exception:
            return default

    def _to_str(self, v):
        if v is None:
            return None
        # lists/dicts (except 'links' where we handle separately)
        if isinstance(v, (list, tuple)):
            # join strings cleanly; else repr for mixed types
            if all(isinstance(x, str) or x is None for x in v):
                return " | ".join([x for x in v if x])
            return str(v)
        if isinstance(v, (bytes, bytearray)):
            try:
                return v.decode("utf-8", "replace")
            except Exception:
                return v.decode("latin-1", "replace")
        return str(v)

    def _to_links(self, v):
        if v is None:
            return []
        if not isinstance(v, (list, tuple)):
            v = [v]
        return list(dict.fromkeys(str(x) for x in v if x is not None))

    def _open_writer(self):
        # This function ensures the writer is open.
        # If it's already open, it does nothing.
        # If it's closed (or None), it opens a new one.
        if self._writer is None:
            self._writer = pq.ParquetWriter(
                str(self._parquet_path),
                self._schema,
                compression="zstd",
                use_dictionary=True,
            )

    def _write_table(self, table, spider):
        self._open_writer()
        self._writer.write_table(table)
        self._written_count += table.num_rows

    def _flush_batch(self, spider):
        if not self._rows:
            return
        
        try:
            # Create the table from the current batch of rows
            table = pa.Table.from_pylist(self._rows, schema=self._schema)
            
            # Ensure the writer is open and write the table
            self._open_writer()
            self._writer.write_table(table)
            
            # **FIX 1: Update the written count**
            # This was the main bug. The count was not updated on a successful batch.
            self._written_count += table.num_rows

        except Exception as e:
            spider.logger.error(
                "Batch write failed; attempting per-row salvage: %s", e, exc_info=True
            )
            # Salvage row-by-row; quarantine the truly bad ones
            self._open_writer()
            with self._quarantine_path.open("a", encoding="utf-8") as qf:
                for r in self._rows:
                    try:
                        t = pa.Table.from_pylist([r], schema=self._schema)
                        self._writer.write_table(t)
                        self._written_count += 1
                    except Exception as row_e:
                        self._dropped_count += 1
                        spider.logger.warning("Dropping 1 bad row: %s", row_e)
                        qf.write(json.dumps(r, ensure_ascii=False) + "\n")
        finally:
            # **FIX 2: Clear the rows**
            # This ensures rows are not processed again on the next flush.
            self._rows.clear()

    # ---------- scrapy hooks

    def process_item(self, item, spider):
        scraped_texts = item.get("scraped_text") or []
        num_pages = len(scraped_texts)

        # If there are no scraped pages, we still process one "row"
        # which will contain the error information.
        if num_pages == 0 and item.get("error"):
             num_pages = 1
             scraped_texts = [""] # Create a dummy entry to process the error row

        for c in range(num_pages):
            url = self._safe_idx(item.get("scraped_urls"), c, "")
            timestamp = datetime.fromtimestamp(time.time()).strftime("%c")

            row = DualExporter()
            # single-value / vector fields
            row["ID"] = self._to_str(self._safe_idx(item.get("ID"), 0, ""))
            row["dl_rank"] = int(c)  # guaranteed int
            row["dl_slot"] = self._to_str(self._safe_idx(item.get("dl_slot"), 0, None))
            row["alias"] = self._to_str(self._safe_idx(item.get("alias"), 0, ""))
            
            # Handle error. If num_pages is 0, this will be the only loop.
            row["error"] = self._to_str(item.get("error"))  # may be list/string/None
            
            row["redirect"] = self._to_str(self._safe_idx(item.get("redirect"), 0, ""))
            row["start_page"] = self._to_str(
                self._safe_idx(item.get("start_page"), 0, "")
            )

            row["url"] = self._to_str(url)
            row["timestamp"] = timestamp

            # per-page lists
            row["title"] = self._to_str(
                self._safe_idx(item.get("title"), c, "")
            ).strip()
            row["description"] = self._to_str(
                self._safe_idx(item.get("description"), c, "")
            ).strip()
            row["keywords"] = self._to_str(
                self._safe_idx(item.get("keywords"), c, "")
            ).strip()
            row["language"] = self._to_str(
                self._safe_idx(item.get("language"), c, "")
            ).strip()
            row["links"] = self._coerce_str(self._to_links(item.get('links') or []))
            row["html_path"] = self._to_str(
                self._safe_idx(item.get("html_path"), c, "")
            )

            text_val = self._safe_idx(scraped_texts, c, "")
            # If you want to strip HTML entities/tags, do it here. Keeping as-is:
            row["text"] = self._to_str(text_val)

            # self._rows.append(dict(row))
            self._rows.append(self._sanitize_row(dict(row)))

            # periodic flush
            # Use len(self._rows) instead of self._written_count for batch logic
            if len(self._rows) >= self._BATCH_SIZE:
                self._flush_batch(spider)

        return item

    def close_spider(self, spider):
        # Flush any remaining rows
        self._flush_batch(spider)
        
        # Close the writer if it's open
        if self._writer is not None:
            self._writer.close()
            self._writer = None

        # **FIX 3: Removed the broken compaction logic.**
        # The pipeline is designed to write to a single file directly.
        # The logic for 'parts' was non-functional as _parts_dir was never written to.
        # The single file at self._parquet_path is now the final, correct output.

        # timing / stats
        try:
            stats = spider.crawler.stats
            elapsed = stats.get_value("elapsed_time_seconds")
            if elapsed is None:
                start_time = stats.get_value("start_time")
                if start_time:
                    now = datetime.now(tz=getattr(start_time, "tzinfo", None))
                    elapsed = (now - start_time).total_seconds()
                else:
                    elapsed = time.perf_counter() - self._t0
            save_spider_duration(spider.name, elapsed, self._duration_path)
        except Exception as e:
            spider.logger.error("Error saving duration: %s", e, exc_info=True)

        spider.logger.info(
            "Parquet done: wrote %d rows%s%s",
            self._written_count,
            (
                f", dropped {self._dropped_count} (see {self._quarantine_path.name})"
                if self._dropped_count
                else ""
            ),
            f", file: {self._parquet_path}" if self._written_count else "",
        )

