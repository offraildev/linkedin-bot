import os
import argparse
from pathlib import Path

from dotenv import load_dotenv
import pandas as pd
from tqdm import tqdm
from langchain.llms import Ollama

from loggers import log_message_with_traceback


load_dotenv()
DE_TOOLS_LOG = os.getenv("DE_TOOLS_LOG")
JOBS_DATAFRAME = os.getenv("JOBS_DATAFRAME")
DE_TOOLS_OUT = os.getenv("DE_TOOLS_OUT")


def extract_de_tools(job_description: str) -> str:
    prompt = f"""Below is a job description for the role of a data engineer:\n`{job_description}`\n
    List the Data engineering software tools mentioned in the job description. 
    Give only the list, no explanations needed."""
    return llm(prompt)


def process_and_save(data: pd.DataFrame, start_idx: int) -> None:
    data = data.loc[~data["Job_description"].isnull()].reset_index(drop=True)
    for idx, row in tqdm(
        data.iloc[start_idx:].iterrows(), total=len(data.iloc[start_idx:])
    ):
        try:
            job_description = row["Job_description"].strip()
            with open(Path(DE_TOOLS_OUT) / f"{row['Job_id']}.txt", "w") as file:
                file.write(extract_de_tools(job_description))
        except Exception as _:
            log_message_with_traceback(DE_TOOLS_LOG, f"failed index: {idx}")


def get_args():
    parser = argparse.ArgumentParser(
        prog="extract_de_tools",
        description="Extract Data Engineering tools from job descriptions and save to plain text files.",
    )
    parser.add_argument(
        "--llm_type",
        type=str,
        help="LLM model to use for extraction.",
        default="llama2",
    )
    parser.add_argument(
        "--start_idx",
        type=int,
        help="Start index to continue processing from a certain point in the csv.",
        default=0,
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()

    llm = Ollama(model=args.llm_type)
    jobs_df = pd.read_csv(JOBS_DATAFRAME)

    process_and_save(jobs_df, start_idx=args.start_idx)
    print("Processing Completed!!!!!!!!")
