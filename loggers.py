import traceback
from datetime import datetime


def log_message_with_traceback(log_path: str, message: str) -> None:
    with open(log_path, "a") as file:
        timestamp = datetime.now().strftime("%d/%m/%y %H:%M:%S")
        file.write(f"{timestamp}: {message}")
        traceback.print_exc(file=file)
        file.write("\n")


def log_message(log_path: str, message: str) -> None:
    with open(log_path, "a") as file:
        timestamp = datetime.now().strftime("%d/%m/%y %H:%M:%S")
        file.write(f"{timestamp}: {message}")
        file.write("\n")
