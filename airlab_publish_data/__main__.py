"""
Upload a folder to AirLab Data Server

Adapted from the script snippet from "Publish a Dataset on AirLab Shared Server" on AirLab slite.
"""

# MinIO Python SDK example for uploading a directory.
import os
import json
import typing as T
from pathlib import Path
from rich import progress
from minio import Minio
from minio.error import S3Error


T_target = T.Literal[
    "airlab-share-01.andrew.cmu.edu:9000", 
    "airlab-share-02.andrew.cmu.edu:9000"
]


def main(credential: Path, bucket_name: str, source_folder: Path, target: T_target):
    assert credential.exists()
    assert source_folder.exists()
    
    # Create a client with the MinIO server playground, its access key
    # and secret key.
    with open(credential, "r") as f: keys = json.load(f) 
    
    client = Minio(target,
        access_key=keys["accessKey"],
        secret_key=keys["secretKey"],
        secure=True # Updated by Yaoyu @ 20240903
    )

    # Make the bucket if it doesn't exist.
    found = client.bucket_exists(bucket_name)
    assert found, f"Cannot find the bucket requested {bucket_name} on target {target}"

    def upload_folder(folder_path, bucket_name, client):
        for root, dirs, files in os.walk(folder_path):
            for filename in progress.track(files, description="Uploading files"):
                source_file = os.path.join(root, filename)
                destination_file = os.path.relpath(source_file, folder_path)
                destination_file = os.path.join(os.path.basename(folder_path), destination_file)
                print(f"Uploading {source_file} to {destination_file}...")
                try:
                    # Attempt to get the object, which will raise an error if it doesn't exist
                    stat_object = client.stat_object(bucket_name, destination_file)
                    print(f"File {destination_file} already exists in bucket {bucket_name}. Skipping...")
                except S3Error as e:
                    # If the error is specific to the file not existing, proceed with upload
                    if e.code == 'NoSuchKey':
                        client.fput_object(bucket_name, destination_file, source_file)
                        print(source_file, "successfully uploaded as object", destination_file, "to bucket", bucket_name)
                    else:
                        # Handle other errors
                        print("Error:", e)

    # Upload the entire folder
    upload_folder(source_folder, bucket_name, client)


if __name__ == "__main__":
    from argparse import ArgumentParser
    parser = ArgumentParser(prog="MinIO Bucket Uploader")
    parser.add_argument("--key", type=Path, help="Path to the credential file (usually named credential.json)")
    parser.add_argument("--src", type=Path, help="The directory you want to upload to the bucket")
    parser.add_argument("--dst", type=str,  help="Destination bucket name")
    parser.add_argument("--endpoint", type=str, choices=T.get_args(T_target), default="airlab-share-01.andrew.cmu.edu:9000")
    args = parser.parse_args()
    
    main(args.key, args.dst, args.src, args.endpoint)
