from prefect import flow
from prefect.variables import Variable
from prefect_aws import AwsCredentials, S3Bucket
from unstructured.partition.auto import partition
from unstructured.documents.elements import Element
from unstructured.staging.base import elements_to_json
import json

from src.graphql.api_client.client import UpdateSourceInput
from src.cli import setup_cli
from src.utils.api import api_connect
from src.utils.logging.loggers import get_logger


@flow
def source_unstructured(source_id: list[str], **kwargs):
    """
    Processes a source using the Unstructured library.
    """
    log = get_logger()

    client, user = api_connect()

    # Load the AWS credentials
    aws = AwsCredentials.load("do-spaces")
    bucket = Variable.get("spaces_public_bucket")
    if not bucket or bucket == "":
        raise ValueError(
            "No bucket name provided. Please set the 'spaces_public_bucket' variable."
        )

    # Create the S3 bucket client
    s3 = S3Bucket(
        bucket_name=bucket,
        credentials=aws,
    )

    if len(source_id) == 0:
        raise ValueError("No source id provided")
    op = client.get_source(source_id[0])
    if not op:
        raise ValueError(f"Source {source_id[0]} not found")
    source = op.get_source
    partitions: list[Element] = None
    if source.location:
        partitions = partition(url=source.location, languages=["eng", "swe"])
    for p in partitions:
        if p.metadata:
            log.info(f"Metadata: {p.metadata.to_dict()}")
        if p.text:
            log.info(f"Text: {p.text}")
        if p.category:
            log.info(f"Category: {p.category}")
    update_source = UpdateSourceInput(id=source.id)
    update_source.content = {"unstructured": json.loads(elements_to_json(partitions))}
    op = client.update_source(update_source)
    log.info(f"Updated source {source_id[0]} with Unstructured data.")


if __name__ == "__main__":

    def args(parser):
        parser.add_argument(
            "source_id",
            type=str,
            nargs="+",
            help="The source ID to process.",
        )

    setup_cli(source_unstructured, args)
