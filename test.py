from collections import defaultdict
from datetime import datetime, timezone
from pprint import pprint
from urllib.parse import urlparse, urlunparse
import base64
import boto3
import h5py
import json
import numpy as np
import os
import re
import requests
import shutil
import time

auth_file = "auth.json"

if os.environ.get("FOX_BACKUP_CONF") is None:
    auth = json.load(open(auth_file, "r"))
else:
    auth = json.loads(base64.b64decode(os.environ.get("FOX_BACKUP_CONF")))

s3_endpoint = auth["s3"]["endpoint"]
aws_access_key_id = auth["s3"]["accessKeyId"]
aws_secret_access_key = auth["s3"]["secretAccessKey"]
bucket_name = auth["s3"]["bucketName"]
ipfs_endpoint = auth["s3"]["ipfsEndpoint"]
s3_client = boto3.client(
    "s3",
    endpoint_url=s3_endpoint,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

def download_cid(cid, output_file_path):
    url = f"{ipfs_endpoint}/{cid}"
    with requests.get(url, stream=True) as r:
        with open(output_file_path, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

def download_file_from_bucket(object_name, output_file_path):
    try:
        cid = s3_client.head_object(
            Bucket=bucket_name,
            Key=object_name,
        ).get("Metadata").get("cid")
        print(f"{cid}")
        download_cid(cid, output_file_path)
        print(f"File {bucket_name}/{object_name} downloaded successfully.")
    except Exception as e:
        print(f"Failed to download the file {object_name}:", e)
        raise e

download_file_from_bucket("Automne-2023.hdf5", "downloads_backup/Automne-2023.hdf5")
