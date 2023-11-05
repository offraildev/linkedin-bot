import os
import json
import multiprocessing as mp
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
JOB_IDS = os.getenv("JOB_IDS")
COMPANY_METAS = os.getenv("COMPANY_METAS")
JOB_METAS = os.getenv("JOB_METAS")
JOBS_PROCESSED = os.getenv("JOBS_PROCESSED")


def get_job_title(job_id: int, job_metas: dict) -> str | None:
    try:
        return job_metas[job_id]["data"]["title"]
    except (IndexError, KeyError):
        return None


def get_company_type(job_id: int, company_metas: dict) -> str | None:
    try:
        return company_metas[job_id]["included"][1]["name"]
    except (IndexError, KeyError):
        return None


def get_job_description(job_id: int, job_metas: dict) -> str | None:
    try:
        return job_metas[job_id]["data"]["description"]["text"]
    except (IndexError, KeyError):
        return None


def get_company_name(job_id: int, company_metas: dict) -> str | None:
    try:
        return company_metas[job_id]["included"][2]["name"]
    except (IndexError, KeyError):
        return None


def get_company_url(job_id: int, job_metas: dict) -> str | None:
    try:
        if "url" in job_metas[job_id]["included"][0]:
            return job_metas[job_id]["included"][0]["url"]
        return job_metas[job_id]["included"][1]["url"]
    except (IndexError, KeyError):
        return None


def get_company_description(job_id: int, company_metas: dict) -> str | None:
    try:
        return company_metas[job_id]["included"][2]["description"]
    except (IndexError, KeyError):
        return None


def get_employee_count(job_id: int, company_metas: dict) -> str | None:
    try:
        return company_metas[job_id]["included"][2]["employeeCount"]
    except (IndexError, KeyError):
        return None


def process_job(job_id: int, job_metas: dict, company_metas: dict) -> dict:
    return {
        "job_id": job_id,
        "job_title": get_job_title(job_id, job_metas),
        "company_name": get_company_name(job_id, company_metas),
        "company_type": get_company_type(job_id, company_metas),
        "job_description": get_job_description(job_id, job_metas),
        "company_description": get_company_description(job_id, company_metas),
        "company_url": get_company_url(job_id, job_metas),
        "employee_count": get_employee_count(job_id, company_metas),
    }


def parallel_process(job_ids: int, company_metas: dict, job_metas: dict) -> list[dict]:
    pool = mp.Pool(os.cpu_count())
    result = pool.starmap(
        process_job,
        [(job_id, job_metas, company_metas) for job_id in job_ids],
    )
    pool.close()
    pool.join()
    return result


def load_data() -> tuple[list[int], dict, dict]:
    if JOB_IDS is None or COMPANY_METAS is None or JOB_METAS is None:
        raise Exception("One or more required environment variables are not defined.")

    with open(JOB_IDS, "r") as file:
        job_id = file.readlines()
    job_ids = [id.strip() for id in job_id]

    with open(COMPANY_METAS, "r") as file:
        company_metas = json.load(file)

    with open(JOB_METAS, "r") as file:
        job_metas = json.load(file)

    return job_ids, company_metas, job_metas


def compile_job_meta_data() -> None:
    job_ids, company_metas, job_metas = load_data()
    jobs_dict = parallel_process(job_ids, company_metas, job_metas)
    jobs_df = pd.DataFrame(jobs_dict)
    jobs_df.to_csv(JOBS_PROCESSED, index=False)


if __name__ == "__main__":
    compile_job_meta_data()
