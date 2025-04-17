import requests
import pandas as pd
import polars as pl
from google.cloud import storage
import requests
import xml.etree.ElementTree as ET
import glob
from time import time
from collections import Counter
import pyarrow as pa
import pyarrow.parquet as pq

def list_of_files(program, ti):
    print(ti)

    # Direct S3 URL for file listing
    s3_url = "https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk?list-type=2&max-keys=1000"

    response = requests.get(s3_url)

    if response.status_code == 200:
        root = ET.fromstring(response.text)
        
        file_urls = []
        
        for content in root.findall(".//{http://s3.amazonaws.com/doc/2006-03-01/}Contents"):
            key = content.find("{http://s3.amazonaws.com/doc/2006-03-01/}Key").text
            if key[-3:] == 'csv' and key.split('/')[0] == program:
                
                file_urls.append(f"https://cycling.data.tfl.gov.uk/{key}")
    else:
        print(f"Failed to fetch S3 bucket contents. Status code: {response.status_code}")

    return file_urls


def download_files(**kwargs):
    ti = kwargs['ti']
    DATASET_PATH = kwargs['dataset_path']
    file_urls = ti.xcom_pull(task_ids='fetch_file_urls') 
    print(len(file_urls))
    for url in file_urls:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        save_path = f'{DATASET_PATH}/{url.split('/')[-1]}'
        print(save_path)
        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

def process_active_travel_counts(**kwargs):
    t1 = time()
    column_mapping = {
        "Wave": "wave",
        "SiteId": "site_id",
        "SiteID": "site_id",
        "Date": "date",
        "Weather": "weather",
        "Time": "time",
        "Day": "day",
        "Round": "round",
        "Direction": "direction",
        "Path": "path",
        "Mode": "mode",
        "Count": "count"
    }
    DATASET_PATH = kwargs['dataset_path']
    FILE_NAME = kwargs['file_name']
    csv_files = glob.glob(f"{DATASET_PATH}/*.csv")
    
    final_df = None

    for file in csv_files:
        try:
            df_base = pl.read_csv(file, dtypes={'SiteID': pl.Utf8})

            df = df_base.rename(column_mapping, strict=False)

            df = df.with_columns(
                pl.col("date").str.to_date("%d/%m/%Y").alias("date")
            )
            df = df.with_columns(
                pl.col("time")
                .str.strip_chars()
                .str.strptime(pl.Time)
            )


            final_df = df if final_df is None else final_df.vstack(df)
        except Exception as e:
            df = pl.read_csv(file)
            print(file, df.columns)
    if final_df is not None:
        final_df.write_parquet(f"{DATASET_PATH}/{FILE_NAME}.parquet")
        print(f"Saved {len(csv_files)} files into bike_history.parquet!")
        t2 = time()
        print(f"It took {t2 - t1:.3f} seconds to process the files and convert to parquet!")
    else:
        print("No files were processed.") 

def process_usage_stats(**kwargs):

    DATASET_PATH = kwargs['dataset_path']
    csv_files = glob.glob(f"{DATASET_PATH}/*.csv")
    
    column_mapping = {
        "Rental Id":"rental_id",
        "Duration":"duration",
        "Bike Id":"bike_id",
        "End Date":"end_date",
        "EndStation Id":"endstation_id",
        "EndStation Name":"endstation_name",
        "Start Date":"start_date",
        "StartStation Id":"startstation_id",
        "StartStation Name":"startstation_name",
        "EndStation Logical Terminal": "endstation_id",
        "StartStation Logical Terminal": "startstation_id",
        "Number": "rental_id",
        "End station number": "endstation_id",
        "End station": "endstation_name",
        "Start station number": "startstation_id",
        "Start station": "startstation_name",
        "Bike number": "bike_id",
        "Total duration (ms)": "duration",
        "End date": "end_date",
        "Start date": "start_date",
        "Duration_Seconds": "duration",
        "End Station Id": "endstation_id",
        "End Station Name": "endstation_name",
        "Start Station Id": "startstation_id",
        "Start Station Name": "startstation_name",
    }
    needed_cols = []
    for v in column_mapping.values():
        if v not in needed_cols:
            needed_cols.append(v)

    csv_files = glob.glob(f"{DATASET_PATH}/*.csv")

    int_columns = set(["Total duration (ms)","Duration_Seconds", "Duration"])
    date_columns = set(["Start Date", "Start date", "End date", "End Date"])
    str_columns = set(column_mapping.keys()) - int_columns - date_columns

    needed_cols = list(dict.fromkeys(column_mapping.values()))  # Preserve order & uniqueness

    
    count = 0
    for idx, file in enumerate(csv_files):
        t11 = time()
        try:
            df = pl.read_csv(file, dtypes={**{col: pl.Utf8 for col in str_columns}, **{col: pl.Datetime for col in date_columns}})

            schema_type = 2 if "Number" in df.columns else 1

            # Rename columns
            col_renamed = {col: column_mapping[col] for col in df.columns if col in column_mapping}
            df = df.rename(col_renamed)

            # Filter only needed columns
            df = df.select([col for col in needed_cols if col in df.columns])

            # Duration adjustment
            if schema_type == 2 and "duration" in df.columns:
                df = df.with_columns((pl.col("duration") // 1000).alias("duration"))

            # Skip malformed files
            if df.shape[1] != 9:
                continue

            df.write_parquet(f"{file[:-4]}.parquet")
            count += 1
        except Exception as e:
            print(e)
            print(file)

        t22 = time()
        print(f"finished processing {idx + 1} / {len(csv_files)} file, which took {t22-t11:.3f} seconds, {file}")

    print(f"Processed {count}  csv files into parquet!")



def dev_info_usage_stats_cols(**kwargs):
    DATASET_PATH = kwargs['dataset_path']
    csv_files = glob.glob(f"{DATASET_PATH}/*.csv")
    c = Counter()
    for idx, file in enumerate(sorted(csv_files)):
        try:
            df_base = pd.read_csv(file)
            c[",".join(df_base.columns)] += 1
            print(list(df_base.columns), idx, file, len(c))

        except Exception as e:
            # df = pl.read_csv(file)
            print(e)
            print("error", file)

    for key, value in c.items():
        print(key, value)

