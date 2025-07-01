from prefect import flow
from prefect.variables import Variable
import os
from prefect_aws import AwsCredentials, S3Bucket

from src.cli import setup_cli
from src.utils.api import api_connect
from src.graphql.api_client.client import CreateSourceInput, UpdateSourceInput
from src.utils.logging.loggers import get_logger


@flow
def upload_source(file: list[str], **kwargs):
    """
    Creates a source in the database and uploads attached files to S3 if any.
    """
    log = get_logger()

    client, user = api_connect()

    # Load the AWS credentials
    aws = AwsCredentials.load("digitalocean-spaces")
    bucket = Variable.get("spaces_sources_bucket")
    if not bucket or bucket == "":
        raise ValueError(
            "No bucket name provided. Please set the 'spaces_sources_bucket' variable."
        )

    # Create the S3 bucket client
    s3 = S3Bucket(
        bucket_name=bucket,
        credentials=aws,
    )
    # Test if the bucket is accessible
    s3.list_objects(max_items=1)

    file_path = file[0] if len(file) > 0 else None
    source_type = None
    location = ""
    content = None
    upload = True
    if file_path is not None:
        if file_path.endswith(".pdf"):
            source_type = "PDF"
        elif (
            file_path.endswith(".png")
            or file_path.endswith(".jpg")
            or file_path.endswith(".jpeg")
        ):
            source_type = "IMAGE"
        elif file_path.endswith(".txt"):
            source_type = "TEXT"
            f_stat = os.stat(file_path)
            if f_stat.st_size <= 512 * 1024:
                upload = False
                with open(file_path, "r") as f:
                    content = {"text": f.read()}
        elif file_path.startswith("https://") or file_path.startswith("http://"):
            source_type = "URL"
            location = file_path
            upload = False
        else:
            source_type = "FILE"
    else:
        log.error("No file provided.")
        return

    source = CreateSourceInput(type=source_type)
    source.location = location
    source.content = content

    op = client.add_source(source)
    if not op.create_source.source.id:
        log.error("Error creating source")
        return
    source_id = op.create_source.source.id
    log.info(f"Source created with ID: {source_id}")
    if upload:
        log.info(f"Uploading file {file_path} to S3 bucket {bucket}")
        to_path = f"{source_id}{os.path.splitext(file_path)[1]}"
        s3.upload_from_path(
            from_path=file_path,
            to_path=to_path,
            ExtraArgs={"ACL": "public-read"},
        )
        log.info(f"File uploaded to S3 bucket {bucket} at {to_path}")
        source_update = UpdateSourceInput(id=source_id)
        source_update.location = (
            f"https://{bucket}.fra1.cdn.digitaloceanspaces.com/{to_path}"
        )
        op = client.update_source(source_update)
        log.info(f"Source updated with location: {op.update_source.source.location}")


if __name__ == "__main__":

    def args(parser):
        parser.add_argument(
            "file",
            type=str,
            nargs="*",
            help="The file to upload.",
        )

    setup_cli(upload_source, args)
