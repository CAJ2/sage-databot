#!/usr/bin/env python3
"""
Local helper script for uploading a file to S3 and triggering
the Windmill upload_source script.

Usage:
    python src/sources/upload_local.py <local_file_path> [--workspace <workspace>]

Prerequisites:
    - s3cmd configured with DigitalOcean Spaces credentials
    - wmill CLI authenticated (run `wmill workspace add` first)

The script uploads the file to the databot S3 bucket under a temporary key,
then calls the Windmill `f/sources/upload_source` script with the resulting
S3Object so that creds are only handled by s3cmd and Windmill.
"""

import argparse
import subprocess
import sys
import uuid
import os
import json


S3_BUCKET = "sage-leaf-databot"
S3_ENDPOINT = "https://fra1.digitaloceanspaces.com"
WINDMILL_SCRIPT_PATH = "f/sources/upload_source"


def upload_to_s3(local_path: str) -> str:
    """
    Upload a local file to the databot S3 bucket under a temporary key.
    Returns the S3 key (path within the bucket).
    """
    ext = os.path.splitext(local_path)[1]
    temp_key = f"__upload_tmp/{uuid.uuid4()}{ext}"
    s3_url = f"s3://{S3_BUCKET}/{temp_key}"

    print(f"Uploading {local_path} to {s3_url} ...")
    subprocess.run(
        [
            "s3cmd",
            "put",
            "--host",
            S3_ENDPOINT,
            "--host-bucket",
            "%(bucket)s.fra1.digitaloceanspaces.com",
            local_path,
            s3_url,
        ],
        check=True,
    )
    print(f"Upload complete: {s3_url}")
    return temp_key


def run_windmill_script(s3_key: str, workspace: str | None):
    """
    Trigger the Windmill upload_source script with the S3Object pointing
    to the uploaded file.
    """
    s3_object = {"s3": s3_key}
    args_json = json.dumps({"file": s3_object})

    cmd = [
        "wmill",
        "script",
        "run",
        "--path",
        WINDMILL_SCRIPT_PATH,
        "--args",
        args_json,
    ]
    if workspace:
        cmd += ["--workspace", workspace]

    print(f"Running Windmill script: {WINDMILL_SCRIPT_PATH}")
    subprocess.run(cmd, check=True)


def main():
    parser = argparse.ArgumentParser(
        description="Upload a local file and trigger Windmill upload_source."
    )
    parser.add_argument("file", help="Path to the local file to upload")
    parser.add_argument(
        "--workspace",
        default=None,
        help="Windmill workspace to use (defaults to wmill CLI default)",
    )
    args = parser.parse_args()

    if not os.path.isfile(args.file):
        print(f"Error: file not found: {args.file}", file=sys.stderr)
        sys.exit(1)

    s3_key = upload_to_s3(args.file)
    run_windmill_script(s3_key, args.workspace)


if __name__ == "__main__":
    main()
