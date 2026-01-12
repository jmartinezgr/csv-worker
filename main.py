import time
import traceback

from config.settings import get_settings
from services.api import make_api_client
from services.jobs import get_next_job, process_job, start_heartbeat


def log(msg: str):
    print(f"[WORKER] {msg}")


def main():
    settings = get_settings()
    client = make_api_client(settings)

    hb_thread, stop_event = start_heartbeat(
        client,
        settings.job_id,
        settings.heartbeat_interval,
        log,
    )

    log(f"Worker ID: {settings.worker_id}")
    log(f"Processing job: {settings.job_id}")

    max_empty_retries = 2
    sleep_seconds = 60
    empty_retries = 0

    try:
        while True:
            next_job = get_next_job(client, log)

            if next_job:
                empty_retries = 0  # reset retries when a job is found
                process_job(next_job, settings, client, log)
                continue

            empty_retries += 1
            if empty_retries > max_empty_retries:
                log("No job available after retries. Exiting.")
                return

            log(
                f"No job available. Retry {empty_retries}/{max_empty_retries} after {sleep_seconds}s."
            )
            time.sleep(sleep_seconds)
    except Exception as e:
        log(f"Unhandled error: {e}\n{traceback.format_exc()}")
        raise
    finally:
        stop_event.set()
        hb_thread.join(timeout=5)


if __name__ == "__main__":
    main()
