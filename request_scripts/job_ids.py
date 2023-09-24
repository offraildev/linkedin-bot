import os
import re
import json
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
SESSIONID = os.getenv("SESSIONID")
JOB_ID_REQUEST_META = os.getenv("JOB_ID_REQUEST_META")


def strip_id(string_with_id: str) -> int:
    return int(re.search(r"\((\d+)\,", string_with_id).group(1))


def read_request_meta() -> tuple[dict, dict]:
    meta = json.loads(Path(JOB_ID_REQUEST_META).read_text())

    cookies = meta["cookies"]
    cookies["JSESSIONID"] = f"ajax:{SESSIONID}"

    headers = meta["headers"]
    headers["csrf-token"] = f"ajax:{SESSIONID}"
    return cookies, headers


def request_job_ids(count: int, start: int) -> dict:
    cookies, headers = read_request_meta()
    return requests.get(
        f"https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollection-177&count={count}&q=jobSearch&query=(origin:JOB_SEARCH_PAGE_OTHER_ENTRY,keywords:data%20engineer,spellCorrectionEnabled:true)&start={start}",
        cookies=cookies,
        headers=headers,
    ).json()


def extract_job_ids(count: int, start: int = 0) -> dict:
    response = request_job_ids(count, start)
    return {
        "ids": [
            strip_id(element["jobCardUnion"]["*jobPostingCard"])
            for element in response["data"]["elements"]
        ],
        "curr_start": response["data"]["paging"]["start"],
        "curr_count": response["data"]["paging"]["count"],
    }
