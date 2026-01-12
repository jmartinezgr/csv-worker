"""Simple API client with base URL and default headers.

Provides a small wrapper around `requests.Session` so you can do:

        client = make_api_client()
        client.get("/jobs/123")

It automatically applies:
 - Base URL from settings
 - Default headers with API secret and worker signature
 - Sensible timeout defaults
"""

from __future__ import annotations

from typing import Any, Mapping

import requests

from config.settings import get_settings, Settings


class ApiClient:
    """Minimal API client with default headers and base URL."""

    def __init__(
        self,
        base_url: str,
        api_secret: str,
        worker_id: str,
        *,
        session: requests.Session | None = None,
        default_timeout: float | int = 20,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.default_timeout = default_timeout
        # Default headers include an authentication secret and a worker signature
        self.default_headers: dict[str, str] = {
            "X-API-Secret": api_secret,
            "X-Worker-Id": worker_id,
            "X-Client-Signature": f"csv-worker/{worker_id}",
            "Accept": "application/json",
            "User-Agent": f"csv-worker/{worker_id}",
        }

    def _full_url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return f"{self.base_url}/{path.lstrip('/')}"

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
        data: Any | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | int | None = None,
    ) -> requests.Response:
        req_headers = {**self.default_headers, **(headers or {})}
        resp = self.session.request(
            method=method,
            url=self._full_url(path),
            params=params,
            json=json,
            data=data,
            headers=req_headers,
            timeout=timeout or self.default_timeout,
        )
        resp.raise_for_status()
        return resp

    def get(self, path: str, **kwargs: Any) -> requests.Response:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> requests.Response:
        return self.request("POST", path, **kwargs)

    def put(self, path: str, **kwargs: Any) -> requests.Response:
        return self.request("PUT", path, **kwargs)

    def patch(self, path: str, **kwargs: Any) -> requests.Response:
        return self.request("PATCH", path, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> requests.Response:
        return self.request("DELETE", path, **kwargs)


def make_api_client(settings: Settings | None = None) -> ApiClient:
    """Factory that builds a client from environment settings."""
    cfg = settings or get_settings()
    return ApiClient(
        base_url=cfg.backend_url,
        api_secret=cfg.api_secret,
        worker_id=cfg.worker_id,
    )
