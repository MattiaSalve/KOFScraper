from pathlib import Path
from tempfile import NamedTemporaryFile
import csv, os, time
from filelock import FileLock

CSV_PATH = Path("/Volumes/WD_BLACK_SN770/KOF/python/ARGUS/spider_durations.csv")
LOCK_PATH = CSV_PATH.with_suffix(".csv.lock")


def save_spider_duration(spider_name: str, duration_seconds: float):
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

    with FileLock(str(LOCK_PATH), timeout=30):
        rows = {}
        if CSV_PATH.exists():
            with CSV_PATH.open("r", newline="", encoding="utf-8") as f:
                r = csv.reader(f)
                header = next(r, None)
                for name, dur in r:
                    rows[name] = dur

        rows[spider_name] = f"{duration_seconds:.3f}"

        with NamedTemporaryFile(
            "w", delete=False, dir=str(CSV_PATH.parent), newline="", encoding="utf-8"
        ) as tmp:
            w = csv.writer(tmp)
            w.writerow(["spider", "duration_s"])
            for name, dur in sorted(rows.items()):
                w.writerow([name, dur])
            tmp_name = tmp.name

        os.replace(tmp_name, CSV_PATH)
