import os
from urllib.parse import urlparse
from prefect.variables import Variable
from prefect_aws import AwsCredentials, S3Bucket
from src.utils.logging.loggers import get_logger


def is_production() -> bool:
    """
    Check if the environment is production.
    """
    return "PREFECT_ENV" in os.environ and os.environ["PREFECT_ENV"] == "production"


def download_cache_file(basepath_var: str, url: str) -> str:
    """
    Download a file from a URL and cache it based on the local cache_dir.

    Args:
        basepath_var (str): The name of the variable that stores the base path.
        url (str): The URL to download the file from.

    Returns:
        str: The local path or URL to the downloaded file.
    """
    log = get_logger()

    cache_dir = Variable.get("cache_dir")
    if cache_dir is None:
        raise ValueError("Variable cache_dir is not set.")

    # Create the directory if it doesn't exist
    os.makedirs(cache_dir, exist_ok=True)

    filename = os.path.basename(url)
    filepath = os.path.join(cache_dir, filename)

    basepath: str = Variable.get(basepath_var)
    if not basepath:
        basepath = cache_dir
    baseparsed = urlparse(basepath)
    if baseparsed.scheme.startswith("s3"):
        # If the basepath is an S3 URL, cache from S3
        # or if it doesn't exist in S3, download it
        # and save back to S3
        localpath = os.path.join(basepath, filename)
        creds_key = Variable.get("aws_credentials_key")
        creds = AwsCredentials.load(creds_key if creds_key else "digitalocean-spaces")
        s3_bucket = S3Bucket(bucket_name=baseparsed.netloc, credentials=creds)
        try:
            s3_bucket.download_object_to_path(baseparsed.path, localpath)
        except Exception:
            log.info(f"File {localpath} not found in S3, downloading from {url}.")
            if not os.path.exists(localpath):
                os.system(f"curl -o {localpath} {url}")
            else:
                log.info(f"File {localpath} already exists, skipping download.")
            uploadpath = s3_bucket.upload_from_path(
                localpath, os.path.join(baseparsed.path, filename)
            )
            log.info(f"File {localpath} uploaded to S3 at {uploadpath}.")
        return localpath
    elif baseparsed.scheme == "" or baseparsed.scheme == "file":
        if not os.path.isabs(basepath):
            basepath = os.path.join(os.getcwd(), basepath)
        filepath = os.path.join(basepath, filename)

        if not os.path.exists(filepath):
            log.info(f"Downloading {url} to {filepath}")
            os.system(f"curl -o {filepath} {url}")
        else:
            log.info(f"File {filepath} already exists, skipping download.")

    return filepath
