# requirements: project

import wmill
import os
from smart_open import open as smart_open
from urllib.request import urlretrieve

from f.utils.s3 import S3Client


def main():
    """
    Download the Whos On First global countries data from Geocode Earth.
    """
    download_url = "https://data.geocode.earth/wof/dist/legacy/whosonfirst-data-country-latest.tar.bz2"
    s3client = S3Client()

    filename = os.path.basename(download_url)
    s3_file = wmill.S3Object(s3=os.path.join("wof", "countries", filename))
    filepath = os.path.join(".", filename)

    if s3client.s3_exists(s3_file.s3):
        print(f"File {s3_file} already exists, skipping download.")
        return s3_file

    if not os.path.exists(filepath):
        print(f"Downloading {download_url} to {filepath}")
        urlretrieve(download_url, filepath)
    else:
        print(f"File {filepath} already exists, skipping download.")

    print(f"Uploading {filepath} to S3")
    with open(filepath, "rb") as f_in:
        with smart_open(
            f"s3://{s3client.bucket}/{s3_file.s3}",
            "wb",
            transport_params={"client": s3client.client},
        ) as f_out:
            for chunk in iter(lambda: f_in.read(1024 * 1024), b""):
                f_out.write(chunk)
    print(f"Wrote {filepath} to {s3_file.s3}")

    return s3_file
