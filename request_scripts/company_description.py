import os
import json
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
SESSIONID = os.getenv("SESSIONID")
COMPANY_DESC_REQUEST_META = os.getenv("COMPANY_DESC_REQUEST_META")


def read_request_meta() -> tuple[dict, dict]:
    meta = json.loads(Path(COMPANY_DESC_REQUEST_META).read_text())

    cookies = meta["cookies"]
    cookies["JSESSIONID"] = f"ajax:{SESSIONID}"

    headers = meta["headers"]
    headers["csrf-token"] = f"ajax:{SESSIONID}"
    return cookies, headers


def request_company_description(job_id: int) -> dict:
    cookies, headers = read_request_meta()
    headers["referer"] = headers["referer"].replace("PLACEHOLDER", f"{job_id}")
    return requests.get(
        f"https://www.linkedin.com/voyager/api/graphql?includeWebMetadata=true&variables=(cardSectionTypes:List(COMPANY_CARD),jobPostingUrn:urn%3Ali%3Afsd_jobPosting%3A{job_id})&&queryId=voyagerJobsDashJobPostingDetailSections.4c4bfb9c98ca3794a7fb78d0be504ea0",
        cookies=cookies,
        headers=headers,
    ).json()
