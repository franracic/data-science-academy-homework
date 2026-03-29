import pandas
import numpy
import glob
import os
import logging

logging.basicConfig(level=logging.INFO)

files = sorted(glob.glob(os.path.join("raw_data", "**", "*.csv"), recursive=True))

cols = [
    "event_date",
    "event_name",
    "user_pseudo_id",
    "platform",
    "status",
    "geo_country",
    "id",
]

event_cols = [
    "event_date",
    "event_timestamp",
    "event_name",
    "user_pseudo_id",
    "geo_country",
    "app_info_version",
    "platform",
    "status",
    "id",
    "event_id",
    "name",
    "item_name",
    "previous_first_open_count",
    "firebase_experiments",
]

output = "processed_data.csv"
first = True

for file in files:
    logging.info(f"Processing: {file}")

    try:
        chunks = pandas.read_csv(
            file,
            header=None,
            names=event_cols,
            usecols=cols,
            chunksize=50000,
        )

        for chunk in chunks:
            chunk = chunk.iloc[
                numpy.where(
                    ~(
                        numpy.isin(chunk["geo_country"].values, ["Germany", "Italy"])
                        & (chunk["event_name"].values == "drawer_action")
                    )
                )
            ]

            chunk["event_date"] = pandas.to_datetime(
                chunk["event_date"], format="%Y%m%d", errors="coerce"
            )
            chunk = chunk[chunk["event_date"] <= "2024-03-15"]

            chunk = chunk.rename(
                columns=lambda x: x.split("_")[0]
                + "".join(word.capitalize() for word in x.split("_")[1:])
            )

            chunk.to_csv(output, mode="w" if first else "a", index=False, header=first)
            first = False

    except Exception as e:
        logging.error(f"Error processing {file}: {e}")

logging.info("Complete!")
