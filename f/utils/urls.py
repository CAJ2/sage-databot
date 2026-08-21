# requirements: project

SOURCES_CDN_PREFIX = "cdn://sources/"
SOURCES_PUBLIC_BASE_URL = "https://sources.sageleaf.app/"


def normalize_source_url(source_url: str) -> str:
    if source_url.startswith(SOURCES_CDN_PREFIX):
        return source_url.replace(SOURCES_CDN_PREFIX, SOURCES_PUBLIC_BASE_URL, 1)
    return source_url
