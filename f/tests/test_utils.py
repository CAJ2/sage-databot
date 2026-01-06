# requirements: project

import f.utils.db.crdb as crdb
from f.utils.s3 import S3Client

def test_db_sage():
    engine = crdb.create_sql_engine()
    with engine.begin() as conn:
        conn.execute("SELECT 1")
    print("Connection successful")

def test_s3_client():
    s3 = S3Client()
    assert not s3.s3_exists("path/to/nonexistant/file.txt")
    with open("test.txt", "wb") as f:
        f.write(b"test\n")
    with open("test.txt", "rb") as f:
        s3.s3_upload("test.txt", f)
    assert s3.s3_exists("test.txt")

def main():
    test_s3_client()
    test_db_sage()
    return 0
