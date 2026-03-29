import zipfile
import os
import logging
from config import pipeline, REMOTE

logging.basicConfig(level=logging.INFO)
os.makedirs("raw_data/march", exist_ok=True)
os.makedirs("march/", exist_ok=True)

TARGET = "march"


def extract(sftp):
    zips = [f for f in sftp.listdir(f"{REMOTE}/{TARGET}") if f.endswith(".zip")]
    logging.info(f"{len(zips)} zips found")

    for zipf in zips:
        remote = f"{REMOTE}/{TARGET}/{zipf}"
        local = f"{TARGET}/{zipf}"
        if zipf in os.listdir(TARGET) and sftp.stat(remote).st_size == os.path.getsize(
            local
        ):
            logging.info(f"{zipf} already exists.")
            continue

        sftp.get(remote, local)

        with zipfile.ZipFile(local, "r") as zipr:
            if not zipr.namelist():
                logging.error("Zip file is empty.")
                continue

            zipr.extractall(f"raw_data/{TARGET}")
        logging.info(f"{zipf} extracted successfully.")
    return True


pipeline(TARGET, extract)
