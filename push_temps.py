from collections import defaultdict
from datetime import datetime, timezone
from pprint import pprint
import boto3
import codecs
import h5py
import json
import numpy as np
import os
import re
import requests
import shutil

auth = json.load(open("auth.json", "r"))
sigfox_login = auth["sigfox"]["login"]
sigfox_pswd = auth["sigfox"]["password"]
sigfox_devid = auth["sigfox"]["deviceId"]
sigfox_endpoint = f"https://{sigfox_login}:{sigfox_pswd}@api.sigfox.com/v2"

s3_endpoint = "https://s3.filebase.com"
aws_access_key_id = auth["filebase-s3"]["accessKeyId"]
aws_secret_access_key = auth["filebase-s3"]["secretAccessKey"]
bucket_name = auth["filebase-s3"]["bucketName"]
s3_client = boto3.client(
    "s3",
    endpoint_url=s3_endpoint,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

H5_DATASET_NAME = "lanloup_temps"
NP_DTYPE = [
    ("timestamp", np.ulonglong),
    ("data", "<S8"),
    ("seqNum", np.ulonglong),
    ("lqi", np.short),
]


def get(path):
    return requests.get(f"{sigfox_endpoint}/{path}").json()


def get_one_page_msgs():
    messages = get(f"devices/{sigfox_devid}/messages")
    # pprint(messages)
    return np.array(
        list(
            map(
                lambda x: (
                    int(x["time"]),
                    codecs.decode(x["data"], "hex_codec"),
                    int(x["seqNumber"]),
                    int(x["lqi"]),
                ),
                messages["data"],
            )
        ),
        dtype=NP_DTYPE,
    )


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
        print("File uploaded successfully.")
    except Exception as e:
        print("Failed to upload the file:", e)


def download_file_from_bucket(object_name, output_file_path):
    try:
        s3_client.download_file(bucket_name, object_name, output_file_path)
        print("File downloaded successfully.")
    except Exception as e:
        print("Failed to download the file:", e)


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


def download_all_files():
    make_clean_dir("downloads")
    files = list_files_in_bucket() or []
    print(f"[LOG] bucket files: {files}")
    for f in files:
        path = f"downloads/{f}"
        download_file_from_bucket(f, path)
    return files


def read_hdf5_to_numpy(file_path, dataset_name):
    try:
        with h5py.File(file_path, "r") as hdf_file:
            dataset = hdf_file[dataset_name][:]
            return np.array(dataset)
    except Exception as e:
        print("Error reading the HDF5 file:", e)
        return None


def download_seasons_historic():
    downloaded_files = download_all_files()
    seasons_dict = dict()
    for f in downloaded_files:
        path = f"downloads/{f}"
        seasons_dict[f[:-5]] = read_hdf5_to_numpy(path, H5_DATASET_NAME)
    return seasons_dict


def merge_by_seqnum(arr1, arr2):
    merged_array = np.concatenate((arr1, arr2))
    merged_array.sort(order="seqNum")
    unique_indices = np.unique(merged_array["seqNum"], return_index=True)[1]
    new = 2 * len(unique_indices) - len(merged_array)
    unique_merged_array = merged_array[unique_indices]
    return unique_merged_array, new


seasons_historic = download_seasons_historic()
print(f"[LOG] seasons_historic: {seasons_historic}")
msgs = get_one_page_msgs()
classified_msgs = classify_messages_by_season_year(
    sorted(
        map(
            lambda x: (datetime.fromtimestamp(x[0] // 1000, timezone.utc), *x[1:]),
            msgs.tolist(),
        )
    )
)
for season, msgs in classified_msgs.items():
    make_clean_dir("results")
    historic = seasons_historic.get(season)
    if historic is None:
        historic = np.empty(shape=(0,), dtype=NP_DTYPE)
    mergedmsgs, new = merge_by_seqnum(historic, np.array(msgs, dtype=NP_DTYPE))
    print(f"[LOG] added {new} new entries to {season}")
    write_msgs_to_hdf5(f"results/{season}.hdf5", mergedmsgs)
    delete_file_from_bucket(f"{season}.hdf5")
    upload_file_to_bucket(f"results/{season}.hdf5", f"{season}.hdf5")
