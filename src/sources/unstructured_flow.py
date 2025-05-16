from prefect import flow, task
from prefect.variables import Variable
from prefect_aws import AwsCredentials, S3Bucket
from unstructured.partition.auto import partition
from unstructured.documents.elements import Element
from unstructured.staging.base import (
    elements_to_json,
    _fix_metadata_field_precision,
    elements_to_dicts,
)
import json
import fasttext
import os

from src.graphql.api_client.client import UpdateSourceInput
from src.cli import setup_cli
from src.utils.api import api_connect
from src.utils.logging.loggers import get_logger


@task
def detect_language(text: str) -> str:
    """
    Detects the language of the given text using FastText.
    """
    model = fasttext.load_model("data/lid.176.bin")
    predictions = model.predict(text)
    return predictions[0][0].split("__")[-1]


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

    temp_dir = Variable.get("cache_dir")
    if not temp_dir or temp_dir == "":
        raise ValueError(
            "No temp directory provided. Please set the 'cache_dir' variable."
        )
    if not os.path.exists(os.path.join(temp_dir, "unstructured")):
        os.makedirs(os.path.join(temp_dir, "unstructured"))

    if len(source_id) == 0:
        raise ValueError("No source id provided")
    op = client.get_source(source_id[0])
    if not op:
        raise ValueError(f"Source {source_id[0]} not found")
    source = op.get_source
    partitions: list[Element] = None
    langs = []
    if source.location:
        partitions = partition(
            url=source.location,
            detect_language_per_element=True,
            extract_images_in_pdf=False,
            skip_infer_table_types=["jpg", "png", "heic"],
        )
    for p in partitions:
        if p.text and len(p.text) > 100:
            try:
                lang = detect_language(p.text)
                if lang not in langs:
                    langs.append(lang)
                    log.info(f"Detected language: {lang}")
            except Exception as e:
                log.error(f"Error detecting language: {e}")
    precision_adjusted_elements = _fix_metadata_field_precision(partitions)
    element_dicts = elements_to_dicts(precision_adjusted_elements)
    elem_json = json.dumps(
        element_dicts, indent=None, sort_keys=True, ensure_ascii=False
    )
    update_source = UpdateSourceInput(id=source.id)
    if len(elem_json) > 200_000:
        temp_file = os.path.join(temp_dir, "unstructured", f"{source.id}_content.json")
        with open(temp_file, "w") as f:
            f.write(elem_json)
        s3.upload_from_path(
            from_path=temp_file,
            to_path=f"{source.id}_content.json",
            ExtraArgs={"ACL": "public-read"},
        )
        update_source.content_url = (
            f"https://{bucket}.fra1.cdn.digitaloceanspaces.com/{source.id}_content.json"
        )
    else:
        update_source.content = {"unstructured": json.loads(elem_json)}
    if not source.metadata:
        source.metadata = {}
    source.metadata["languages"] = langs
    update_source.metadata = source.metadata
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
