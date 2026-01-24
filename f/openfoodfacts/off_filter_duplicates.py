# requirements: project

import duckdb

from f.utils.s3 import S3Client


def main(parquet_file: str):
    """
    Load an OpenFoodFacts Parquet file into DuckDB and rewrite it with a unique code.
    """
    s3 = S3Client()
    parquet_url = s3.create_url(parquet_file)
    duckdb.sql(s3.duckdb_setup())
    dup_codes = duckdb.sql(
        f"SELECT code FROM read_parquet('{parquet_url}') EXCEPT ALL SELECT DISTINCT code FROM read_parquet('{parquet_url}')"
    ).fetchall()
    if not dup_codes:
        print("No duplicate codes found in the Parquet file.")
        return
    print(f"Total number of duplicates in Parquet file: {len(dup_codes)}")

    # Rewrite the Parquet file with unique codes
    not_in_expr = "', '".join([str(code[0]) for code in dup_codes if len(code[0]) > 0])
    new_url = parquet_url.replace(".parquet", "_dedup.parquet")
    copy_query = f"""
        COPY (
            SELECT * FROM read_parquet('{parquet_url}')
            WHERE code NOT IN ('{not_in_expr}')
        ) TO '{new_url}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50_000)
    """
    duckdb.sql(copy_query)
