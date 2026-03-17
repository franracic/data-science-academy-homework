import tarfile
import os
import logging
from config import pipeline, REMOTE

logging.basicConfig(level=logging.INFO)
os.makedirs("raw_data/", exist_ok=True)

path = "february.tar.gz"


def extract(sftp):
    sftp.get(f"{REMOTE}/{path}", path)
    logging.info(f"{path} downloaded successfully.")

    with tarfile.open(path, "r:gz") as tar:
        if not tar.getnames():
            logging.error("File is empty.")
            return False

        tar.extractall("raw_data/")
    logging.info("File extracted successfully.")
    return True


pipeline(path, extract)
