# requirements: project

import json

import wmill

from f.utils.s3 import S3Client

S3_RESOURCE = "f/s3_config/s3_posthog"


def main() -> dict[str, int]:
    dl = wmill.ducklake()

    rows = dl.query("""
        SELECT manifest_key FROM posthog_import_log
        WHERE imported_at::TIMESTAMP < now()::TIMESTAMP - INTERVAL '1 day'
    """).fetch()

    stale_keys = [row["manifest_key"] for row in (rows or [])]
    print(f"Found {len(stale_keys)} stale manifests to clean up")

    if not stale_keys:
        return {"deleted_manifests": 0, "deleted_files": 0}

    s3 = S3Client(resource_id=S3_RESOURCE)
    deleted_manifests = 0
    deleted_files = 0

    for manifest_key in stale_keys:
        try:
            try:
                obj = s3.client.get_object(Bucket=s3.bucket, Key=manifest_key)
                data = json.loads(obj["Body"].read())
                keys_to_delete = [{"Key": k} for k in data["files"]] + [
                    {"Key": manifest_key}
                ]
                s3.client.delete_objects(
                    Bucket=s3.bucket, Delete={"Objects": keys_to_delete}
                )
                deleted_files += len(keys_to_delete)
                print(f"Deleted {len(keys_to_delete)} files for {manifest_key}")
            except Exception:
                print(f"Manifest already gone: {manifest_key}, removing log entry only")

            escaped_key = manifest_key.replace("'", "''")
            dl.query(
                f"DELETE FROM posthog_import_log WHERE manifest_key = '{escaped_key}'"
            ).execute()
            deleted_manifests += 1
        except Exception as e:
            print(f"Error cleaning up {manifest_key}: {e}")
            continue

    return {"deleted_manifests": deleted_manifests, "deleted_files": deleted_files}
