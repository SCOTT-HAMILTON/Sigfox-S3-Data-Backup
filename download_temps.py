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
ipfs_endpoint = auth["s3"]["ipfsEndpoint"]
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
        download_cid(cid, output_file_path)
        print(f"File {bucket_name}/{object_name} downloaded successfully.")
    except Exception as e:
        print(f"Failed to download the file {object_name}:", e)
        raise e

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
