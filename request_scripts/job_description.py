import os
import json
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
SESSIONID = os.getenv("SESSIONID")
JOB_DESC_REQUEST_META = os.getenv("JOB_DESC_REQUEST_META")


def read_request_meta() -> tuple[dict, dict]:
    meta = json.loads(Path(JOB_DESC_REQUEST_META).read_text())

    cookies = meta["cookies"]
    cookies["JSESSIONID"] = f"ajax:{SESSIONID}"

    headers = meta["headers"]
    headers["csrf-token"] = f"ajax:{SESSIONID}"
    return cookies, headers


def request_job_description(job_id: int) -> dict:
    cookies, headers = read_request_meta()
    headers["referer"] = headers["referer"].replace("PLACEHOLDER", f"{job_id}")
    return requests.get(
        f"https://www.linkedin.com/voyager/api/jobs/jobPostings/{job_id}?decorationId=com.linkedin.voyager.deco.jobs.web.shared.WebLightJobPosting-23&",
        cookies=cookies,
        headers=headers,
    ).json()
