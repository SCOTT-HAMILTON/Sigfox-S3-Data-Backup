from collections import defaultdict
from datetime import datetime, timezone
from pprint import pprint
from urllib.parse import urlparse, urlunparse
import argparse
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

parser = argparse.ArgumentParser(
    description="Python script to push sigfox temps to s3"
)
parser.add_argument("--auth", type=str, help="Path to the auth json file")
parser.add_argument("--debug-data", type=str, help="Path to the debug json file")
parser.add_argument("-d", "--debug", help="Debug mode",
                    action="store_true")
args = parser.parse_args()
auth_file = args.auth or "auth.json"

if os.environ.get("FOX_BACKUP_CONF") is None:
    auth = json.load(open(auth_file, "r"))
else:
    auth = json.loads(base64.b64decode(os.environ.get("FOX_BACKUP_CONF")))
sigfox_login = auth["sigfox"]["login"]
sigfox_pswd = auth["sigfox"]["password"]
sigfox_devid = auth["sigfox"]["deviceId"]
sigfox_endpoint = f"https://{sigfox_login}:{sigfox_pswd}@api.sigfox.com/v2"

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

H5_DATASET_NAME = "lanloup_temps"
NP_DTYPE = [
    ("timestamp", np.ulonglong),
    ("data", "|V4"),
    ("seqNum", np.ulonglong),
    ("lqi", np.short),
]
NP_DTYPE_EXPORT = [
    ("timestamp", np.ulonglong),
    ("data", np.string_),
    ("seqNum", np.ulonglong),
    ("lqi", np.short),
]


def get(url, fullUrl = False):
    if fullUrl:
        return requests.get(url).json()
    else:
        return requests.get(f"{sigfox_endpoint}/{url}").json()

def add_login_password_to_url(url, login, password):
    parsed_url = urlparse(url)
    updated_netloc = f"{login}:{password}@{parsed_url.hostname}"
    if parsed_url.port:
        updated_netloc += f":{parsed_url.port}"
    updated_url = urlunparse(
        (
            parsed_url.scheme,
            updated_netloc,
            parsed_url.path,
            parsed_url.params,
            parsed_url.query,
            parsed_url.fragment,
        )
    )
    return updated_url


def get_one_page_msgs(url=None):
    if url == None:
        messages = get(f"devices/{sigfox_devid}/messages")
    else:
        messages = get(
            add_login_password_to_url(url, sigfox_login, sigfox_pswd)
            ,fullUrl = True
        )
    nextLink = messages["paging"].get("next")
    return (
        np.array(
            list(
                map(
                    lambda x: (
                        int(x["time"]),
                        int(x["data"], 16).to_bytes(4, "big"),
                        int(x["seqNumber"]),
                        int(x["lqi"]),
                    ),
                    messages["data"],
                )
            ),
            dtype=NP_DTYPE,
        ),
        nextLink,
    )


def get_all_pages_msgs():
    if args.debug_data:
        with open(args.debug_data, 'rb') as f:
            results = np.load(f)
            print(f"[LOG] Loaded results ({results.shape[0]} msgs) directly from {args.debug_data}:\n{results[0]}")
            return results
    allmsgs = []
    url = None
    pages = 0
    while True:
        pages += 1
        msgs, nextLink = get_one_page_msgs(url)
        allmsgs.append(msgs)
        url = nextLink
        if url is None:
            break
        else:
            time.sleep(1)
    results = np.concatenate(allmsgs)
    print(f"[LOG] fetched {pages} pages for {results.shape[0]} messages.")
    if args.debug:
        dbg_msg = "debug-msgs.npy"
        with open(dbg_msg, 'wb') as f:
            np.save(f, results)
        print(f"[DEBUG] messages saved for debugging to {dbg_msg}")
    return results


def write_msgs_to_hdf5(hdf5_file, data):
    with h5py.File(hdf5_file, "w") as file:
        dataset = file.create_dataset(H5_DATASET_NAME, data=data)


def get_season(dt):
    # Function to get the season based on the month of the datetime object
    month = dt.month
    if 3 <= month <= 5:
        return "Printemps"
    elif 6 <= month <= 8:
        return "Été"
    elif 9 <= month <= 11:
        return "Automne"
    else:
        return "Hiver"


def classify_messages_by_season_year(message_list):
    # Dictionary to store messages grouped by "season-year"
    messages_by_season_year = defaultdict(list)
    for msg in message_list:
        date = msg[0]
        season = get_season(date)
        year = date.year
        season_year_key = f"{season}-{year}"
        messages_by_season_year[season_year_key].append(
            (int(date.timestamp()), *msg[1:])
        )

    # Sort messages in each group by date
    for key, messages in messages_by_season_year.items():
        messages_by_season_year[key] = sorted(messages, key=lambda x: x[0])

    return dict(messages_by_season_year)


def list_files_in_bucket():
    pattern = r"(Printemps|Été|Automne|Hiver)-\d{4}\.hdf5"
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    if "Contents" in response:
        data = [obj["Key"] for obj in response["Contents"]]
        return list(filter(lambda f: re.fullmatch(pattern, f) != None, data))
    else:
        print("Failed to list files.")
        return None


def upload_file_to_bucket(file_path, object_name):
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"File {bucket_name}/{object_name} uploaded successfully.")
    except Exception as e:
        print(f"Failed to upload {bucket_name}/{object_name}@{file_path}:", e)

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

def delete_file_from_bucket(file_path):
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=file_path)
        print(f"File {file_path} deleted successfully.")
    except Exception as e:
        print(f"Failed to delete file {file_path}: {e}")


def make_clean_dir(dir_path):
    try:
        shutil.rmtree(dir_path)
    except (FileExistsError, FileNotFoundError):
        pass
    try:
        os.makedirs(dir_path)
    except (FileExistsError, FileNotFoundError):
        pass


def download_seasons(seasons):
    make_clean_dir("downloads")
    files = [f for f in (list_files_in_bucket() or []) if f[:-5] in seasons]
    print(f"[LOG] bucket files: {files}")
    for f in files:
        path = f"downloads/{f}"
        download_file_from_bucket(f, path)
    return files


def read_hdf5_to_numpy(file_path, dataset_name):
    try:
        with h5py.File(file_path, "r") as hdf_file:
            if not hdf_file:
                return None
            dataset = hdf_file[dataset_name][:]
            return np.array(dataset)
    except Exception as e:
        print("Error reading the HDF5 file:", e)
        raise e


def download_seasons_historic(seasons):
    downloaded_files = download_seasons(seasons)
    seasons_dict = dict()
    for f in downloaded_files:
        path = f"downloads/{f}"
        data = read_hdf5_to_numpy(path, H5_DATASET_NAME)
        seasons_dict[f[:-5]] = data
    return seasons_dict


def merge_by_timestamp(arr1, arr2):
    merged_array = np.concatenate((arr1, arr2))
    merged_array.sort(order="timestamp")
    unique_indices = np.unique(merged_array["timestamp"], return_index=True)[1]

    def count_new():
        mask = ~np.isin(arr1, arr2)
        return len(arr1[mask])

    new = count_new()
    unique_merged_array = merged_array[unique_indices]
    return unique_merged_array, new


def print_np_array(array, max_lines=5):
    if array.shape[0] < max_lines * 2:
        print(array)
    else:
        print(np.concatenate((array[:max_lines], array[-max_lines:])))


msgs = None
try:
    msgs = sorted(
        map(
            lambda x: (datetime.fromtimestamp(x[0] // 1000, timezone.utc), *x[1:]),
            get_all_pages_msgs().tolist(),
        ),
        key=lambda x: x[0],
    )
except Exception as e:
    print(f"[FATAL] couldn't fetch msgs: {e}, exitting...")
    quit()

seasons = set(map(lambda x: f"{get_season(x[0])}-{x[0].year}", msgs))
seasons_historic = None
try:
    seasons_historic = download_seasons_historic(seasons)
except Exception as e:
    print(f"[FATAL] couldn't fetch season's historic: {e}, quitting...")
    quit()

classified_msgs = classify_messages_by_season_year(msgs)
for season, msgs in classified_msgs.items():
    make_clean_dir("results")
    historic = seasons_historic.get(season)
    if historic is None:
        historic = np.empty(shape=(0,), dtype=NP_DTYPE)
    mergedmsgs, new = merge_by_timestamp(np.array(msgs, dtype=NP_DTYPE), historic)
    print(f"[LOG] {season} ({len(msgs)} msgs from sigfox, {len(historic)} from s3):")
    print_np_array(mergedmsgs)
    print(f"[LOG] added {new} new entr{'ies' if new != 1 else 'y'} to {season}")
    if new > 0:
        write_msgs_to_hdf5(f"results/{season}.hdf5", mergedmsgs)
        delete_file_from_bucket(f"{season}.hdf5")
        upload_file_to_bucket(f"results/{season}.hdf5", f"{season}.hdf5")
    else:
        print(f"[LOG] {season} skipped, nothing added")
