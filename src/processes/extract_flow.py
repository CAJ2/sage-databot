from prefect import flow, task
from prefect.utilities.annotations import quote
from unstructured.documents.elements import Element
from unstructured.staging.base import elements_from_json
import fasttext
import spacy
import json

from src.cli import setup_cli
from src.utils.api import api_connect
from src.graphql.api_client.client import CreateSourceInput, UpdateSourceInput
from src.utils.logging.loggers import get_logger
from src.utils.lang import LANG_TO_SPACY_MODEL


@task
def detect_language(text: str) -> str:
    """
    Detects the language of the given text using FastText.
    """
    model = fasttext.load_model("data/lid.176.bin")
    predictions = model.predict(text)
    return predictions[0][0].split("__")[-1]


@task
def detect_regions(nlp, content: list[Element]) -> list[str]:
    """
    Detects regions in the given content using Spacy.
    """
    log = get_logger()

    regions = []
    propn = []
    for p in content:
        if p.text:
            doc = nlp(p.text)
            for w in doc:
                if w.pos_ == "PROPN":
                    propn.append(w.text)
    log.info(f"Searching regions for: {propn}")
    return regions


@flow
def extract_processes_flow(source_id: list[str], **kwargs):
    """
    Creates a source in the database and uploads attached files to S3 if any.
    """
    log = get_logger()

    client, user = api_connect()

    if len(source_id) == 0:
        raise ValueError("No source id provided")
    op = client.get_source(source_id[0])
    if not op:
        raise ValueError(f"Source {source_id[0]} not found")
    source = op.get_source
    if not source.content:
        raise ValueError(f"Source {source_id[0]} has no content")
    if "unstructured" not in source.content:
        raise ValueError(f"Source {source_id[0]} has no Unstructured content")
    content: list[Element] = elements_from_json(
        text=json.dumps(source.content["unstructured"])
    )
    combined_text = ""
    for p in content:
        if p.text:
            combined_text += p.text + " "

    log.info(f"Detecting language for source {source_id[0]} content")
    lang = detect_language(combined_text)
    if lang not in LANG_TO_SPACY_MODEL:
        raise ValueError(f"Detected language {lang} not supported")
    log.info(f"Detected language: {lang}")
    log.info(f"Processing {len(content)} elements")
    nlp = spacy.load(LANG_TO_SPACY_MODEL[lang])
    detect_regions(quote(nlp), content)


if __name__ == "__main__":

    def args(parser):
        parser.add_argument(
            "source_id",
            type=str,
            nargs="+",
            help="The source ID to process.",
        )

    setup_cli(extract_processes_flow, args)
