# requirements: project

import json

import wmill

from f.utils.s3 import S3Client

S3_RESOURCE = "f/s3_config/s3_posthog"


def main(prefix: str = "posthog-events/") -> dict[str, int]:
    s3 = S3Client(resource_id=S3_RESOURCE)
    dl = wmill.ducklake()

    already_imported: set[str] = set()
    try:
        rows = dl.query("SELECT manifest_key FROM posthog_import_log").fetch()
        already_imported = {row["manifest_key"] for row in (rows or [])}
    except Exception as e:
        print(f"No import log yet: {e}")

    manifest_keys = []
    for page in s3.s3_scan(prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("_manifest.json"):
                manifest_keys.append(obj["Key"])

    new_manifests = [k for k in manifest_keys if k not in already_imported]
    print(f"Found {len(manifest_keys)} manifests, {len(new_manifests)} new")

    resource = s3.resource
    assert resource is not None
    secret_sql = f"""
        CREATE OR REPLACE SECRET s3_posthog (
            TYPE s3, PROVIDER config,
            KEY_ID '{resource["accessKey"]}',
            SECRET '{resource["secretKey"]}',
            REGION '{resource["region"]}',
            ENDPOINT '{resource["endPoint"]}'
        );
    """

    total_rows = 0
    for manifest_key in new_manifests:
        try:
            obj = s3.client.get_object(Bucket=s3.bucket, Key=manifest_key)
            parquet_keys = json.loads(obj["Body"].read())["files"]
            urls = [s3.create_url(k) for k in parquet_keys]
            url_list = ", ".join(f"'{u}'" for u in urls)
            escaped_key = manifest_key.replace("'", "''")

            dl.query(f"""
                {secret_sql}
                CREATE TABLE IF NOT EXISTS dl.posthog_events (
                    created_at TIMESTAMPTZ,
                    timestamp TIMESTAMPTZ,
                    event VARCHAR,
                    _inserted_at TIMESTAMPTZ,
                    uuid VARCHAR,
                    distinct_id VARCHAR,
                    elements_chain VARCHAR,
                    person_id VARCHAR,
                    person_properties VARCHAR,
                    properties VARCHAR
                );
                CREATE TABLE IF NOT EXISTS dl.posthog_import_log (
                    manifest_key VARCHAR,
                    imported_at TIMESTAMPTZ DEFAULT now(),
                    row_count BIGINT
                );
                BEGIN TRANSACTION;
                INSERT INTO dl.posthog_events
                SELECT created_at, timestamp, event, _inserted_at, uuid, distinct_id,
                       elements_chain, person_id, person_properties, properties
                FROM read_parquet([{url_list}]);
                INSERT INTO dl.posthog_import_log (manifest_key, row_count)
                SELECT '{escaped_key}', count(*) FROM read_parquet([{url_list}]);
                COMMIT;
            """).execute()

            row_count = (
                dl.query(
                    f"SELECT row_count FROM posthog_import_log WHERE manifest_key = '{escaped_key}'"
                ).fetch_one_scalar()
                or 0
            )
            total_rows += row_count
            print(f"Imported {row_count} rows from {manifest_key}")
        except Exception as e:
            print(f"Error importing {manifest_key}: {e}")
            continue

    return {"processed": len(new_manifests), "rows_imported": total_rows}
