import zipfile
import os
import logging
from config import pipeline, REMOTE

logging.basicConfig(level=logging.INFO)
os.makedirs("raw_data/", exist_ok=True)

path = "january.csv.zip"


def extract(sftp):
    sftp.get(f"{REMOTE}/{path}", path)
    logging.info(f"{path} downloaded successfully.")

    with zipfile.ZipFile(path, "r") as zipf:
        if not zipf.namelist():
            logging.error("Zip file is empty.")
            return False

        zipf.extractall("raw_data/")
    logging.info("File extracted successfully.")
    return True


pipeline(path, extract)
