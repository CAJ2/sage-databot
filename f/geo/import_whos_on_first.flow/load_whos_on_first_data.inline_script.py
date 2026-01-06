# requirements: project

import wmill
import os
import bz2
import boto3
from smart_open import open
from urllib.request import urlretrieve

from f.utils.s3 import S3Client

def main(country: str):
    """
    Download the Whos On First country data from the given URL.
    """
    download_url = f"https://data.geocode.earth/wof/dist/sqlite/whosonfirst-data-admin-{country.lower()}-latest.db.bz2"
    s3client = S3Client()

    # Download the file
    filename = os.path.basename(download_url)
    s3_file = wmill.S3Object(s3=os.path.join("wof", country, filename[:-4]))
    filepath = os.path.join(".", filename)

    if s3client.s3_exists(s3_file.s3):
        print(f"File {s3_file} already exists, skipping download.")
        return s3_file

    if not os.path.exists(filepath):
        print(f"Downloading {download_url} to {filepath}")
        urlretrieve(download_url, filepath)
    else:
        print(f"File {filepath} already exists, skipping download.")

    # Extract the bzip2 file using bz2 python module
    print(f"Extracting {filepath}")
    with bz2.BZ2File(filepath, "rb") as f_in:
        with open(
            f"s3://{s3client.bucket}/{s3_file.s3}", "wb", transport_params={"client": s3client.client}
        ) as f_out:
            for line in f_in:
                f_out.write(line)
    print(f"Wrote {filepath} to {s3_file.s3}")

    return s3_file
