from pprint import pprint
import base64
import boto3
import json
import os
import re
import requests
import shutil

if os.environ.get("FOX_BACKUP_CONF") is None:
    auth = json.load(open("auth.json", "r"))
else:
    auth = json.loads(base64.b64decode(os.environ.get("FOX_BACKUP_CONF")))

s3_endpoint = auth["s3"]["endpoint"]
aws_access_key_id = auth["s3"]["accessKeyId"]
aws_secret_access_key = auth["s3"]["secretAccessKey"]
bucket_name = auth["s3"]["bucketName"]
s3_client = boto3.client(
    "s3",
    endpoint_url=s3_endpoint,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)


def list_files_in_bucket():
    pattern = r"(Printemps|Été|Automne|Hiver)-\d{4}\.hdf5"
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    if "Contents" in response:
        data = [obj["Key"] for obj in response["Contents"]]
        return list(filter(lambda f: re.fullmatch(pattern, f) != None, data))
    else:
        print("Failed to list files.")
        return None


def download_file_from_bucket(object_name, output_file_path):
    try:
        s3_client.download_file(bucket_name, object_name, output_file_path)
        print("File downloaded successfully.")
    except Exception as e:
        print("Failed to download the file:", e)


def make_clean_dir(dir_path):
    try:
        shutil.rmtree(dir_path)
    except (FileExistsError, FileNotFoundError):
        pass
    try:
        os.makedirs(dir_path)
    except (FileExistsError, FileNotFoundError):
        pass


make_clean_dir("downloads_backup")
files = list_files_in_bucket()
print(f"[LOG] bucket files: {files}")
for f in files:
    path = f"downloads_backup/{f}"
    download_file_from_bucket(f, path)
