from typing import Callable
from urllib.parse import urlparse

from azure.storage.blob import BlobServiceClient

from models.file import ChunkStream

Logger = Callable[[str], None]


def parse_blob_components(file_url: str) -> tuple[str, str]:
    """Extract container and blob name from a full https blob URL."""
    parsed = urlparse(file_url)
    path_parts = parsed.path.strip("/").split("/")
    if len(path_parts) < 2:
        raise ValueError("Invalid blob URL path; cannot parse container/blob name")
    container = path_parts[0]
    blob_name = "/".join(path_parts[1:])
    return container, blob_name


def parse_s3_url(s3_url: str) -> tuple[str, str]:
    """Extract bucket and key from s3://bucket/path/to/object."""
    parsed = urlparse(s3_url)
    if parsed.scheme != "s3":
        raise ValueError("S3 URL must start with s3://")
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not bucket or not key:
        raise ValueError("Invalid S3 URL; missing bucket or key")
    return bucket, key


def stream_blob(connection_string: str, file_url: str, log: Logger | None = None):
    """Return a streaming file-like object over the blob (no temp file)."""
    container, blob_name = parse_blob_components(file_url)
    bsc = BlobServiceClient.from_connection_string(connection_string)
    blob_client = bsc.get_blob_client(container=container, blob=blob_name)
    downloader = blob_client.download_blob(max_concurrency=4)
    if log:
        log(f"Streaming blob '{container}/{blob_name}'")
    return ChunkStream(downloader.chunks())
