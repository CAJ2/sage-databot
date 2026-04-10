# requirements: project

"""Compatibility shim kept temporarily during the Typesense migration."""

from f.utils.db import typesense as _typesense

SUPPORTED_LANGS = _typesense.SUPPORTED_LANGS
MeiliClient = _typesense.TypesenseClient
check_create_index = _typesense.check_create_collection
check_create_lang_indexes = _typesense.check_create_aliased_collection
check_lang = _typesense.check_lang
filter_docs_for_lang = _typesense.filter_docs_for_lang
split_docs_by_lang = _typesense.split_docs_by_lang
meili_client = _typesense.ts_client
meili_connect = _typesense.ts_connect
