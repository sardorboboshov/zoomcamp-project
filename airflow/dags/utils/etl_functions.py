import requests
import pandas as pd
from google.cloud import storage
import requests
import xml.etree.ElementTree as ET
import os

def list_of_files(DATASET_PATH, ti):
    print(ti)

    # Direct S3 URL for file listing
    s3_url = "https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk?list-type=2&max-keys=200"

    response = requests.get(s3_url)

    if response.status_code == 200:
        root = ET.fromstring(response.text)
        
        file_urls = []
        
        for content in root.findall(".//{http://s3.amazonaws.com/doc/2006-03-01/}Contents"):
            key = content.find("{http://s3.amazonaws.com/doc/2006-03-01/}Key").text
            if key[-3:] == 'csv':
                file_urls.append(f"https://cycling.data.tfl.gov.uk/{key}")

        # print("\n".join(file_urls))  # Print file URLs
        for file_url in file_urls:
            print(file_url)
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