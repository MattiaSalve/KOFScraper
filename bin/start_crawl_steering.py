import time
import requests
import subprocess
import webbrowser
import os
import pandas as pd
import numpy as np
import math
import pathlib

from datetime import datetime

from .post_run import load_parquet

script_dir = os.path.dirname(__file__)
script_dir_edit = str(script_dir)[:-4]

SCRAPYD_URL = "http://localhost:6800"

def after_all_spiders_finished(path):
    """This function runs after all spiders have finished."""
    print("\nAll spiders have finished. Running post-processing...")
    # >>> Add your custom logic here <<<
    ddf = load_parquet(path)

    pass


def wait_for_spiders_to_finish(project_name, job_ids, path, poll_interval=10):
    """Poll Scrapyd until all given job IDs are finished or failed."""
    print(f"\n⏳ Waiting for {len(job_ids)} spiders to finish...")
    unfinished = set(job_ids)

    while unfinished:
        try:
            resp = requests.get(f"{SCRAPYD_URL}/listjobs.json?project={project_name}")
            data = resp.json()
        except Exception as e:
            print("Error checking job status:", e)
            time.sleep(poll_interval)
            continue

        running_ids = {j['id'] for j in data.get('running', [])}
        pending_ids = {j['id'] for j in data.get('pending', [])}
        finished_ids = {j['id'] for j in data.get('finished', [])}

        # Remove finished or failed jobs
        for job_id in list(unfinished):
            if job_id in finished_ids or (job_id not in running_ids and job_id not in pending_ids):
                unfinished.remove(job_id)

        print(f"Still running: {len(unfinished)} jobs")
        if unfinished:
            time.sleep(poll_interval)

    after_all_spiders_finished(path)


def start_crawl():
    from ARGUS_noGUI import argus_settings

    filepath = argus_settings.filepath
    delimiter = argus_settings.delimiter
    encoding = argus_settings.encoding
    index_col = argus_settings.index_col
    url_col = argus_settings.url_col
    lang = argus_settings.lang
    n_cores = argus_settings.n_cores
    limit = argus_settings.limit
    log_level = argus_settings.log_level
    prefer_short_urls = argus_settings.prefer_short_urls
    pdfscrape = argus_settings.pdfscrape
    run_id = datetime.now().strftime("%Y-%m-%d")


    # Read URLs
    data = pd.read_csv(filepath, delimiter=delimiter, encoding=encoding, on_bad_lines="skip", engine="python")

    # Language ISO handling (unchanged)
    if lang == "None":
        language_ISOs = ""
    else:
        ISO_codes = pd.read_csv(
            script_dir_edit + r"/misc/ISO_language_codes.txt",
            delimiter="\t",
            encoding="utf-8",
            on_bad_lines="skip",
            engine="python",
        )
        language_ISOs_list = (
            ISO_codes.loc[ISO_codes["language"] == lang][["ISO1", "ISO2", "ISO3"]]
            .iloc[0]
            .tolist()
        )
        language_ISOs = ",".join(language_ISOs_list)

    # Split into chunks
    n_url_chunks = math.ceil(len(data) / 10000)
    if n_url_chunks < int(n_cores):
        n_url_chunks = int(n_cores)

    p = 1
    for chunk in np.array_split(data, n_url_chunks):
        chunk.to_csv(
            f"{script_dir_edit}/chunks/url_chunk_p{p}.csv",
            sep=",",
            encoding="utf-8",
        )
        p += 1

    print(f"Splitted URLs into {n_url_chunks} parts.")
    time.sleep(1)

    # Schedule spiders and store job IDs
    job_ids = []
    for p in range(1, n_url_chunks + 1):
        url_chunk = f"{script_dir_edit}/chunks/url_chunk_p{p}.csv"
        curl_cmd = (
            f"curl {SCRAPYD_URL}/schedule.json "
            f"-d project=ARGUS -d spider=dualspider "
            f"-d run_id={run_id} "
            f"-d url_chunk={url_chunk} -d limit={limit} -d ID={index_col} "
            f"-d url_col={url_col} -d language={language_ISOs} "
            f"-d setting=LOG_LEVEL={log_level} "
            f"-d prefer_short_urls={prefer_short_urls} -d pdfscrape={pdfscrape}"
        )

        output = subprocess.check_output(curl_cmd, shell=True).decode("utf-8")
        try:
            import json
            job_id = json.loads(output).get("jobid")
            if job_id:
                job_ids.append(job_id)
        except Exception as e:
            print(f"Could not parse job ID for chunk {p}: {e}")
    
    print(f"Scheduled {len(job_ids)} spiders. Opening web interface...")
    webbrowser.open(f"{SCRAPYD_URL}/", new=0, autoraise=True)

    out_dir = pathlib.Path(__file__).parents[1]
    out_dir = out_dir / f"chunks/run_id={run_id}/parsed"
    # Wait for spiders to finish, then run function
    wait_for_spiders_to_finish("ARGUS", job_ids, out_dir)

    

