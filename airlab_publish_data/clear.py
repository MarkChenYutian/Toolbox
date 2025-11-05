"""
Upload a folder to AirLab Data Server

Adapted from the script snippet from "Publish a Dataset on AirLab Shared Server" on AirLab slite.
"""

# MinIO Python SDK example for uploading a directory.
import json
import typing as T
from pathlib import Path
from rich import progress
from minio import Minio


T_target = T.Literal[
    "airlab-share-01.andrew.cmu.edu:9000", 
    "airlab-share-02.andrew.cmu.edu:9000"
]


def main(credential: Path, bucket_name: str, target: T_target):
    assert credential.exists()
    
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

    objects = [o for o in client.list_objects(bucket_name, recursive=True)]
    for obj in progress.track(objects, description="Clearing"):
        if obj.object_name is None:
            print("skip")
            continue
        client.remove_object(bucket_name, obj.object_name)


if __name__ == "__main__":
    from argparse import ArgumentParser
    parser = ArgumentParser(prog="MinIO Bucket Uploader")
    parser.add_argument("--key", type=Path, help="Path to the credential file (usually named credential.json)")
    parser.add_argument("--dst", type=str,  help="Destination bucket name")
    parser.add_argument("--endpoint", type=str, choices=T.get_args(T_target), default="airlab-share-01.andrew.cmu.edu:9000")
    args = parser.parse_args()
    
    main(args.key, args.dst, args.endpoint)
