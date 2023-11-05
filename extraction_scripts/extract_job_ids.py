import os
import time
import argparse
from dotenv import load_dotenv
from loggers import log_message
from request_scripts.job_ids import request_job_ids

load_dotenv()
JOB_IDS = os.getenv("JOB_IDS")
EXTRACT_JOB_IDS_LOG = os.getenv("EXTRACT_JOB_IDS_LOG")


def write_to_file(job_ids: list[int], file_path: str) -> None:
    with open(file_path, "a") as file:
        file.write(("\n".join([str(id) for id in job_ids])))
        log_message(EXTRACT_JOB_IDS_LOG, "Another batch of job ids written.")


def extract_job_ids(file_path: str, sleep_time: int, batch_size: int) -> None:
    i = 0
    while True:
        try:
            resp = request_job_ids(batch_size, start=i)
        except KeyError:
            break
        write_to_file(resp["ids"], file_path)
        log_message(
            EXTRACT_JOB_IDS_LOG,
            f"last batch written Meta:\nstart: {resp['curr_start']}, count: {resp['curr_count']}",
        )
        i += batch_size
        time.sleep(sleep_time)


def get_args():
    parser = argparse.ArgumentParser(
        prog="extract_job_ids",
        description="Extract job ids in batches and save to file.",
    )
    parser.add_argument(
        "--sleep_time",
        type=int,
        help="Sleep time to use in-between request calls.",
        default=8,
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        help="Count of job ids to request per API call.",
        default=100,
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    extract_job_ids(JOB_IDS)
