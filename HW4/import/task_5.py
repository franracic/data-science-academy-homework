import os
import pandas
from clickhouse_driver import Client
import logging

logging.basicConfig(level=logging.INFO)

USERNAME = "default"
PASSWORD = "admin"
HOST = "clickhouse"
PORT = 9000
TABLE = "l4_dataset"
FILE = "processed_data.csv"

if not os.path.exists(FILE):
    logging.error(f"{FILE} does not exist.")
else:
    try:
        client = Client(host=HOST, port=PORT, user=USERNAME, password=PASSWORD)
        client.execute(
            f"""
        CREATE TABLE IF NOT EXISTS {TABLE}
        (
            eventDate String,
            eventName String,
            userPseudoId String,
            platform LowCardinality(String),
            status LowCardinality(String),
            geoCountry LowCardinality(String),
            id Nullable(Int64)
        )
        ENGINE = MergeTree()
        ORDER BY eventDate
        SETTINGS index_granularity = 8192;
        """
        )
        logging.info(f"Starting upload in chunks")

        chunks = pandas.read_csv(FILE, chunksize=100000)

        total = 0
        for i, chunk in enumerate(chunks):
            for c in chunk.select_dtypes(include=["string", "object"]).columns:
                chunk[c] = chunk[c].astype("object")
            for attempt in range(3):
                try:
                    inserted_rows = client.insert_dataframe(
                        f"INSERT INTO {TABLE} VALUES",
                        chunk,
                        settings=dict(use_numpy=True),
                    )
                    break
                except Exception as e:
                    logging.warning(f"Error on chunk {i+1}, attempt {attempt}")
                    if attempt < 2:
                        logging.info("Retrying...")
                    else:
                        logging.error(f"Failed to insert chunk {i+1}: {e}")
                        raise e

            total += inserted_rows
            logging.info(
                f"Inserted chunk {i + 1} ({inserted_rows} rows). Total: {total}"
            )

        logging.info("Upload complete!")

    except Exception as e:
        logging.error(f"An error occurred during upload: {e}")
