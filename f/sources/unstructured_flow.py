# requirements: extract

import json
import os
import tempfile

import fasttext
from unstructured.documents.elements import Element
from unstructured.partition.auto import partition
from unstructured.staging.base import (
    _fix_metadata_field_precision,  # pyright: ignore[reportPrivateUsage]
    elements_to_dicts,
)

from f.graphql.api_client.input_types import UpdateSourceInput
from f.utils.api import api_connect
from f.utils.s3 import S3Client


def detect_language(text: str) -> str:
    """
    Detects the language of the given text using FastText.
    Assumes the model exists on the worker at `/root/lid.176.bin`.
    """
    model = fasttext.load_model("/root/lid.176.bin")
    predictions = model.predict(text)
    labels: tuple[str, ...] = predictions[0]  # type: ignore[assignment]
    return labels[0].split("__")[-1]  # pyright: ignore[reportGeneralTypeIssues]


def main(source_id: str):
    """
    Processes a source using the Unstructured library.
    Partitions the document, detects language, and stores the result
    back to the source record (inline or via S3 for large content).
    """
    client, _user = api_connect()
    s3 = S3Client(resource_id="f/s3_config/s3_sources")
    bucket = s3.bucket

    op = client.get_source(source_id)
    if not op:
        raise ValueError(f"Source {source_id} not found")
    source = op.source
    if source is None:
        raise ValueError(f"Source {source_id} has no data")

    partitions: list[Element] = []
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
                    print(f"Detected language: {lang}")
            except Exception as e:
                print(f"Error detecting language: {e}")

    precision_adjusted_elements = _fix_metadata_field_precision(partitions)
    element_dicts = elements_to_dicts(precision_adjusted_elements)
    elem_json = json.dumps(
        element_dicts, indent=None, sort_keys=True, ensure_ascii=False
    )
    update_source = UpdateSourceInput(id=source.id)
    if len(elem_json) > 200_000:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".unstructured.json", delete=False
        ) as tmp:
            tmp.write(elem_json)
            temp_file = tmp.name
        try:
            s3_key = f"{source.id}.unstructured.json"
            with open(temp_file, "rb") as f:
                s3.s3_upload(f"s3://{bucket}/{s3_key}", f, mode="wb")
            update_source.content_url = (
                f"https://{bucket}.fra1.cdn.digitaloceanspaces.com/{s3_key}"
            )
        finally:
            os.unlink(temp_file)
    else:
        update_source.content = {"unstructured": json.loads(elem_json)}

    if not source.metadata:
        source.metadata = {}
    source.metadata["languages"] = langs
    update_source.metadata = source.metadata
    client.update_source(update_source)
    print(f"Updated source {source_id} with Unstructured data.")
