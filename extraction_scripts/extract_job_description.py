import os
import time
import json
import argparse
from pathlib import Path

from dotenv import load_dotenv
from loggers import log_message
from request_scripts.job_description import request_job_description


load_dotenv()
JOB_IDS_FILE = os.getenv("JOB_IDS_FILE")
JOB_METAS = os.getenv("JOB_METAS")
EXTRACT_JOB_DESC_LOG = os.getenv("EXTRACT_JOB_DESC_LOG")


def extract_job_description(job_ids: list[int], sleep_time: int) -> None:
    result = {}
    for id in job_ids:
        result[id] = request_job_description(id)
        log_message(EXTRACT_JOB_DESC_LOG, f"last meta collected from index: {id}")
        time.sleep(sleep_time)
    Path(JOB_METAS).write_text(json.dumps(result, indent=4))


def get_args():
    parser = argparse.ArgumentParser(
        prog="extract_job_descriptions",
        description="Extract job descriptions and save to file.",
    )
    parser.add_argument(
        "--sleep_time",
        type=int,
        help="Sleep time to use in-between request calls.",
        default=8,
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    job_ids = list(map(int, Path(JOB_IDS_FILE).read_text().splitlines()))
    extract_job_description(job_ids, args.sleep_time)
