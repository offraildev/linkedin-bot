import os
import pandas as pd
import json
from dotenv import load_dotenv

load_dotenv()
JOB_IDS_FILE = os.getenv("JOB_IDS_FILE")
COMPANY_METAS_FILE = os.getenv("COMPANY_METAS_FILE")
JOB_METAS_FILE = os.getenv("JOB_METAS_FILE")
CSV_OUTPUT_FILE = os.getenv("CSV_OUTPUT_FILE")

def add_job_id(job_id, jobs_dict):
    try:
        jobs_dict["Job_id"].append(job_id)
    except (IndexError, KeyError):
        jobs_dict["Job_id"].append(None)


def add_job_title(job_id, jobs_dict, json_data_job):
    try:
        jobs_dict["Job_title"].append(json_data_job[job_id]["data"]["title"])
    except (IndexError, KeyError):
        jobs_dict["Job_title"].append(None)


def add_company_type(job_id, jobs_dict, json_data_company):
    try:
        jobs_dict["Company_type"].append(
            json_data_company[job_id]["included"][1]["name"]
        )
    except (IndexError, KeyError):
        jobs_dict["Company_type"].append(None)


def add_job_description(job_id, jobs_dict, json_data_job):
    try:
        jobs_dict["Job_description"].append(
            json_data_job[job_id]["data"]["description"]["text"]
        )
    except (IndexError, KeyError):
        jobs_dict["Job_description"].append(None)


def add_company_name(job_id, jobs_dict, json_data_company):
    try:
        jobs_dict["Company_name"].append(
            json_data_company[job_id]["included"][2]["name"]
        )
    except (IndexError, KeyError):
        jobs_dict["Company_name"].append(None)


def add_company_url(job_id, jobs_dict, json_data_job):
    try:
        if "url" in json_data_job[job_id]["included"][0]:
            jobs_dict["Company_url"].append(json_data_job[job_id]["included"][0]["url"])
        else:
            jobs_dict["Company_url"].append(json_data_job[job_id]["included"][1]["url"])
    except (IndexError, KeyError):
        jobs_dict["Company_url"].append(None)


def add_company_desc(job_id, jobs_dict, json_data_company):
    try:
        jobs_dict["Company_description"].append(
            json_data_company[job_id]["included"][2]["description"]
        )
    except (IndexError, KeyError):
        jobs_dict["Company_description"].append(None)


def add_employee_no(job_id, jobs_dict, json_data_company):
    try:
        jobs_dict["No_of_emp"].append(
            json_data_company[job_id]["included"][2]["employeeCount"]
        )
    except (IndexError, KeyError):
        jobs_dict["No_of_emp"].append(None)


def access_and_append(job_ids: list[int], jobs_dict: dict, json_data_company, json_data_job):
    for job_id in job_ids:

        add_job_id(job_id, jobs_dict)

        add_job_title(job_id, jobs_dict, json_data_job)

        add_company_type(job_id, jobs_dict, json_data_company)

        add_job_description(job_id, jobs_dict, json_data_job)

        add_company_name(job_id, jobs_dict, json_data_company)

        add_company_url(job_id, jobs_dict, json_data_job)

        add_company_desc(job_id, jobs_dict, json_data_company)

        add_employee_no(job_id, jobs_dict, json_data_company)
    return jobs_dict


def load_data_from_files():
    
    if JOB_IDS_FILE is None or COMPANY_METAS_FILE is None or JOB_METAS_FILE is None:
        raise Exception("One or more required environment variables are not defined.")

    with open(JOB_IDS_FILE, "r") as file:
        job_id = file.readlines()
    job_ids = [id.strip() for id in job_id]

    with open(COMPANY_METAS_FILE, "r") as json_file:
        json_data_company = json.load(json_file)

    with open(JOB_METAS_FILE, "r") as json_file:
        json_data_job = json.load(json_file)

    return job_ids, json_data_company, json_data_job


def save(jobs_dict):
    jobs_df = pd.DataFrame.from_dict(jobs_dict)

    jobs_df.to_csv(CSV_OUTPUT_FILE+"jobs_df.csv", index=False)


def main():
    job_ids, json_data_company, json_data_job = load_data_from_files()

    jobs_dict = {
        "Job_id": [],
        "Job_title": [],
        "Company_name": [],
        "Company_type": [],
        "Job_description": [],
        "Company_description": [],
        "Company_url": [],
        "No_of_emp": [],
    }

    jobs_dict = access_and_append(job_ids, jobs_dict, json_data_company, json_data_job)
    save(jobs_dict)


if __name__ == "__main__":
    main()
