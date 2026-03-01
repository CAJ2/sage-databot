# requirements: project

import os
import tempfile

from wmill import S3Object

from f.graphql.api_client.client import CreateSourceInput, UpdateSourceInput
from f.graphql.api_client.enums import SourceType
from f.utils.api import api_connect
from f.utils.s3 import S3Client


def main(file: S3Object):
    """
    Creates a source record in the database and uploads the provided S3 file
    to the sources bucket. The file type is inferred from the S3 path extension.
    """
    client, _user = api_connect()
    s3_in = S3Client(resource_id="f/s3_config/s3_databot")
    s3_out = S3Client(resource_id="f/s3_config/s3_sources")

    s3_path = file["s3"]
    ext = os.path.splitext(s3_path)[1].lower()

    source_type: SourceType
    location = ""
    content = None
    upload = True

    if ext == ".pdf":
        source_type = SourceType.PDF
    elif ext in (".png", ".jpg", ".jpeg"):
        source_type = SourceType.IMAGE
    elif ext == ".txt":
        source_type = SourceType.TEXT
        # Small text files are stored inline; larger ones are uploaded to S3
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmp:
            temp_txt = tmp.name
        try:
            with open(temp_txt, "wb") as f_out:
                s3_in.s3_download(f"s3://{s3_in.bucket}/{s3_path}", f_out)
            with open(temp_txt, "rb") as f_in:
                raw = f_in.read()
        finally:
            os.unlink(temp_txt)
        if len(raw) <= 512 * 1024:
            upload = False
            content = {"text": raw.decode("utf-8")}
    else:
        source_type = SourceType.FILE

    source_input = CreateSourceInput(type=source_type)
    source_input.location = location
    source_input.content = content

    op = client.add_source(source_input)
    if not op.create_source or not op.create_source.source:
        raise RuntimeError("Error creating source")
    source_id = op.create_source.source.id
    print(f"Source created with ID: {source_id}")

    if upload:
        dest_key = f"{source_id}{ext}"
        dest_url = f"s3://{s3_out.bucket}/{dest_key}"
        print(f"Uploading file to {dest_url}")
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
            temp_path = tmp.name
        try:
            with open(temp_path, "wb") as f_out:
                s3_in.s3_download(f"s3://{s3_in.bucket}/{s3_path}", f_out)
            with open(temp_path, "rb") as f_in:
                s3_out.s3_upload(dest_url, f_in)
        finally:
            os.unlink(temp_path)

        cdn_url = f"https://{s3_out.bucket}.fra1.cdn.digitaloceanspaces.com/{dest_key}"
        source_update = UpdateSourceInput(id=source_id)
        source_update.location = cdn_url
        update_op = client.update_source(source_update)
        if update_op.update_source and update_op.update_source.source:
            print(
                f"Source updated with location: {update_op.update_source.source.location}"
            )
