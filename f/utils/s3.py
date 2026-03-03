import os
import time
from datetime import timedelta
from typing import Any, Generator
from urllib.parse import urlparse

import boto3
import wmill
from smart_open import open


def _is_test_workspace() -> bool:
    return os.environ.get("WM_WORKSPACE", "").startswith("wm-fork-test")


class S3Client:
    def __init__(self, resource_id="f/s3_config/s3_databot"):
        self.resource_id = resource_id
        self.resource = wmill.get_resource(resource_id)
        if self.resource is None:
            raise ValueError(f"Resource '{resource_id}' does not exist")
        self.bucket = self.resource["bucket"]
        args = wmill.boto3_connection_settings("f/s3_config/s3_databot")
        https_count = args["endpoint_url"].count("https://")
        if https_count > 1:
            args["endpoint_url"] = args["endpoint_url"].replace(
                "https://", "", https_count - 1
            )
        self.client = boto3.client("s3", **args)

    def _ensure_test_prefix(self, path: str) -> str:
        """Prepend __test/ to paths when running in a test workspace."""
        if (
            _is_test_workspace()
            and not path.startswith("__test/")
            and not path.startswith("s3://")
        ):
            return f"__test/{path}"
        return path

    def create_url(self, path: str, bucket="") -> str:
        path = self._ensure_test_prefix(path)
        if bucket == "":
            bucket = self.bucket
        url = self._ensure_url(f"s3://{bucket}/{path}")
        if url is None:
            raise ValueError(f"Invalid path '{path}'")
        return url

    def polars_options(self) -> dict[str, Any]:
        if self.resource is None:
            raise ValueError("S3 resource is not defined")
        endpoint = self.resource.get("endPoint", "")
        if not endpoint.startswith("https://"):
            endpoint = "https://" + endpoint
        options = {
            "access_key_id": self.resource.get("accessKey", ""),
            "secret_access_key": self.resource.get("secretKey", ""),
            "region": self.resource.get("region", ""),
            "endpoint_url": endpoint,
        }
        return options

    def duckdb_setup(self) -> str:
        if self.resource is None:
            raise ValueError("S3 resource is not defined")
        setup = f"""
            CREATE OR REPLACE SECRET secret (
                TYPE s3,
                PROVIDER config,
                KEY_ID '{self.resource.get("accessKey", "")}',
                SECRET '{self.resource.get("secretKey", "")}',
                REGION '{self.resource.get("region", "")}',
                ENDPOINT '{self.resource.get("endPoint", "")}'
            );
        """
        return setup

    def to_wmill(self, url: str) -> wmill.S3Object:
        parsed_url = self._ensure_url(url)
        if parsed_url is None:
            raise ValueError(f"Invalid url '{url}'")
        url_path = parsed_url.replace("s3://", "").replace(self.bucket, "", 1)
        return wmill.S3Object(s3=url_path)

    def s3_exists(self, url: str) -> bool:
        url = self._ensure_test_prefix(url)
        parsed_url = self._ensure_url(url)
        if parsed_url is None:
            raise ValueError(f"Invalid url '{url}'")
        parsed = urlparse(parsed_url)
        path = parsed.path.replace(self.bucket + "/", "", 1)
        response = self.client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=path,
        )
        for obj in response.get("Contents", []):
            if obj["Key"] == path:
                return True
        return False

    def s3_upload(self, url: str, f, mode="wb") -> bool:
        url = self._ensure_test_prefix(url)
        parsed_url = self._ensure_url(url)
        if parsed_url is None:
            return False
        start = time.time()
        print(f"Starting upload to {parsed_url}...")
        with open(parsed_url, mode, transport_params={"client": self.client}) as f_out:
            for line in f:
                f_out.write(line)
        print(
            f"Successfully uploaded to {parsed_url} ({timedelta(seconds=(time.time() - start))})"
        )
        if self.resource_id == "f/s3_config/s3_sources":
            parsed = urlparse(parsed_url)
            key = parsed.path.lstrip("/")
            self.client.put_object_acl(Bucket=self.bucket, Key=key, ACL="public-read")
            print(f"Set public-read ACL on {parsed_url}")
        return True

    def s3_download(self, url: str, f) -> bool:
        url = self._ensure_test_prefix(url)
        parsed_url = self._ensure_url(url)
        if parsed_url is None:
            return False
        start = time.time()
        print(f"Starting download from {parsed_url}...")
        with open(parsed_url, "rb", transport_params={"client": self.client}) as f_in:
            for line in f_in:
                f.write(line)
        print(
            f"Successfully downloaded from {parsed_url} ({timedelta(seconds=(time.time() - start))})"
        )
        return True

    def s3_scan(self, prefix: str) -> Generator[Any, None, None]:
        prefix = self._ensure_test_prefix(prefix)
        paginator = self.client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=prefix)
        for page in page_iterator:
            yield page

    def _ensure_url(self, url: str) -> str | None:
        if "://" not in url:
            url = self.bucket + "/" + url
        try:
            parsed = urlparse(url, "s3")
            return parsed.geturl()
        except Exception:
            print(f"Invalid url: {url}")
            return None
