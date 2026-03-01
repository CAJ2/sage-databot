# requirements: extract

import json

import fasttext  # pyright: ignore[reportMissingImports]
import fasttext.util  # pyright: ignore[reportMissingImports]
import spacy  # pyright: ignore[reportMissingImports]
from unstructured.documents.elements import Element  # pyright: ignore[reportMissingImports]
from unstructured.staging.base import elements_from_json  # pyright: ignore[reportMissingImports]

from f.utils.api import api_connect
from f.utils.lang import LANG_TO_SPACY_MODEL


def detect_language(text: str) -> str:
    """
    Detects the language of the given text using FastText.
    Downloads the lid.176.bin model on first use.
    """
    _ = fasttext.util.download_model("lid.176.bin", if_exists="ignore")
    model = fasttext.load_model("lid.176.bin")
    predictions = model.predict(text)
    labels: tuple[str, ...] = predictions[0]  # type: ignore[assignment]
    return labels[0].split("__")[-1]  # pyright: ignore[reportGeneralTypeIssues]


def detect_regions(nlp, content: list[Element]) -> list[str]:
    """
    Detects regions in the given content using Spacy.
    """
    regions = []
    propn = []
    for p in content:
        if p.text:
            doc = nlp(p.text)
            for w in doc:
                if w.pos_ == "PROPN":
                    propn.append(w.text)
    print(f"Searching regions for: {propn}")
    return regions


def main(source_id: str):
    """
    Extracts process information from a source document.
    Detects language and regions from Unstructured-parsed content.
    """
    client, _user = api_connect()

    op = client.get_source(source_id)
    if not op:
        raise ValueError(f"Source {source_id} not found")
    source = op.source
    if source is None:
        raise ValueError(f"Source {source_id} has no data")
    if not source.content:
        raise ValueError(f"Source {source_id} has no content")
    if "unstructured" not in source.content:
        raise ValueError(f"Source {source_id} has no Unstructured content")
    content: list[Element] = elements_from_json(
        text=json.dumps(source.content["unstructured"])
    )
    combined_text = ""
    for p in content:
        if p.text:
            combined_text += p.text + " "

    print(f"Detecting language for source {source_id} content")
    lang = detect_language(combined_text)
    if lang not in LANG_TO_SPACY_MODEL:
        raise ValueError(f"Detected language {lang} not supported")
    print(f"Detected language: {lang}")
    print(f"Processing {len(content)} elements")
    nlp = spacy.load(LANG_TO_SPACY_MODEL[lang])
    detect_regions(nlp, content)
