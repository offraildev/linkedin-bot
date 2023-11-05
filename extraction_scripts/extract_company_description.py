import os
import json
import time
import argparse
from pathlib import Path

from dotenv import load_dotenv
from loggers import log_message
from request_scripts.company_description import request_company_description

load_dotenv()
JOB_IDS = os.getenv("JOB_IDS")
COMPANY_METAS = os.getenv("COMPANY_METAS")
EXTRACT_COMPANY_DESC_LOG = os.getenv("EXTRACT_COMPANY_DESC_LOG")


def extract_company_description(job_ids: list[int], sleep_time: int) -> None:
    result = {}
    for id in job_ids:
        result[id] = request_company_description(id)
        log_message(EXTRACT_COMPANY_DESC_LOG, f"last meta collected from index: {id}")
        time.sleep(sleep_time)
    Path(COMPANY_METAS).write_text(json.dumps(result, indent=4))


def get_args():
    parser = argparse.ArgumentParser(
        prog="extract_company_descriptions",
        description="Extract company descriptions and save to file.",
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
    job_ids = list(map(int, Path(JOB_IDS).read_text().splitlines()))
    extract_company_description(job_ids, args.sleep_time)
