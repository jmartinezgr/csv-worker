from __future__ import annotations

import threading
import traceback
from typing import Callable

from config.settings import Settings
from models.jobs import JobResponse
from services.api import ApiClient
from services.db import process_csv_stream_to_mongo
from services.storage import stream_blob

Logger = Callable[[str], None]


def start_heartbeat(
    client: ApiClient,
    job_id: str,
    interval: int,
    log: Logger,
) -> tuple[threading.Thread, threading.Event]:
    stop_event = threading.Event()
    thread = threading.Thread(
        target=_heartbeat_loop,
        args=(client, job_id, stop_event, interval, log),
        daemon=True,
    )
    thread.start()
    return thread, stop_event


def _heartbeat_loop(
    client: ApiClient,
    job_id: str,
    stop_event: threading.Event,
    interval: int,
    log: Logger,
):
    """Periodically ping the API to update heartbeat."""
    while not stop_event.is_set():
        try:
            resp = client.put(f"/jobs/{job_id}/heartbeat")
            if resp.status_code != 200:
                log(f"Heartbeat failed: {resp.status_code} {resp.text}")
        except Exception as e:
            log(f"Heartbeat error: {e}")
        stop_event.wait(interval)


def get_next_job(client: ApiClient, log: Logger) -> JobResponse | None:
    try:
        resp = client.get("/jobs/next")
        if resp.status_code == 204:
            return None
        return JobResponse.model_validate(resp.json())
    except Exception as e:
        log(f"Error fetching next job: {e}")
        return None


def mark_complete(
    client: ApiClient,
    job_id: str,
    log: Logger,
    error_message: str | None = None,
):
    params = {"error_message": error_message} if error_message else None
    try:
        resp = client.put(
            f"/jobs/{job_id}/complete",
            params=params,
            timeout=20,
        )
        if resp.status_code != 200:
            log(f"Complete call failed: {resp.status_code} {resp.text}")
        else:
            log("Marked job complete")
    except Exception as e:
        log(f"Error marking job complete: {e}")


def process_job(
    job: JobResponse,
    settings: Settings,
    client: ApiClient,
    log: Logger,
):
    try:
        stream_obj = stream_blob(
            settings.azure_storage_connection_string,
            job.file_path,
            log,
        )

        process_csv_stream_to_mongo(
            stream_obj,
            settings.mongo_uri,
            settings.mongo_database,
            job.collection_name,
            batch_size=settings.batch_size,
            block_size_bytes=settings.block_size_bytes,
            log=log,
        )

        mark_complete(client, job.id, log)
    except Exception as e:
        log(f"{e}\n{traceback.format_exc()}")
        mark_complete(client, job.id, log, error_message=str(e))
        raise
