import zipfile
import os
import logging
from config import pipeline, REMOTE

logging.basicConfig(level=logging.INFO)
os.makedirs("raw_data/march", exist_ok=True)
os.makedirs("march/", exist_ok=True)

path = "march"


def extract(sftp):
    zips = [f for f in sftp.listdir(f"{REMOTE}/{path}") if f.endswith(".zip")]
    logging.info(f"{len(zips)} zips found")

    for zipf in zips:
        path = f"{REMOTE}/{path}/{zipf}"
        if zipf in os.listdir(path) and sftp.stat(
            f"{REMOTE}/{path}/{zipf}"
        ).st_size == os.path.getsize(f"{path}/{zipf}"):
            logging.info(f"{zipf} already exists.")
            continue

        sftp.get(f"{REMOTE}/{path}/{zipf}", f"{path}/{zipf}")

        with zipfile.ZipFile(f"{path}/{zipf}", "r") as zipr:
            if not zipr.namelist():
                logging.error("Zip file is empty.")
                continue

            zipr.extractall(f"raw_data/{path}/")
        logging.info(f"{zipf} extracted successfully.")
    return True


pipeline(path, extract)
