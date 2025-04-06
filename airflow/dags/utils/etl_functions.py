import requests
import pandas as pd
import polars as pl
from google.cloud import storage
import requests
import xml.etree.ElementTree as ET
import glob
from time import time

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
    t1 = time()
    DATASET_PATH = kwargs['dataset_path']
    FILE_NAME = kwargs['file_name']
    csv_files = glob.glob(f"{DATASET_PATH}/*.csv")
    
    final_df = None
    
    def convert_column(col):
        words = col.split()
        return '_'.join(x.lower() for x in words)
    needed_cols = ['rental_id',
                    'duration',
                    'bike_id',
                    'end_date',
                    'endstation_id',
                    'endstation_name',
                    'start_date',
                    'startstation_id',
                    'startstation_name']
    for idx, file in enumerate(csv_files):
        t11 = time()
        try:
            df_base = pl.read_csv(file)
            
            df = df_base.with_columns(
                pl.col("End Date").str.strptime(pl.Datetime).alias("End Date"),
                pl.col("Start Date").str.strptime(pl.Datetime).alias("Start Date")
            ).rename(convert_column)

            df = df.select(needed_cols)
            final_df = df if final_df is None else final_df.vstack(df)
        except Exception as e:
            # df = pl.read_csv(file)
            print(e)
            print(file)
        t22 = time()
        print(f"finished processing {idx + 1} / {len(csv_files)} file, which took {t22-t11:.3f} seconds, {file} ")
    if final_df is not None:
        final_df.write_parquet(f"{DATASET_PATH}/{FILE_NAME}.parquet")
        print(f"Saved {len(csv_files)} files into bike_history.parquet!")
        t2 = time()
        print(f"It took {t2 - t1:.3f} seconds to process the files and convert to parquet!")
    else:
        print("No files were processed.")

def dev_info_usage_stats_cols(**kwargs):
    DATASET_PATH = kwargs['dataset_path']
    csv_files = glob.glob(f"{DATASET_PATH}/*.csv")

    for idx, file in enumerate(sorted(csv_files)):
        try:
            df_base = pd.read_csv(file)
            print(list(df_base.columns), idx, file)
        except Exception as e:
            # df = pl.read_csv(file)
            print(e)
            print("error", file)
def upload_to_gcs(bucket, object_name, local_file, service_account_path):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client.from_service_account_json(service_account_path)
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def web_to_gcs(year, service, init_url, BUCKET):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

        # download it using requests via a pandas df
        request_url = f"{init_url}{service}/{file_name}"
        r = requests.get(request_url)
        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")

        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")