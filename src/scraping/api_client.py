"""Cliente HTTP async para la API ONPE."""
import asyncio
import logging
from typing import Any

import httpx

from src.config import BASE_URL, MAX_RETRIES, REQUEST_DELAY_SECS

logger = logging.getLogger(__name__)


class OnpeClient:
    """Cliente rate-limited para la API de presentación ONPE."""

    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=BASE_URL,
            timeout=30.0,
            headers={"Accept": "application/json"},
        )
        self._last_request = 0.0

    async def _throttle(self) -> None:
        now = asyncio.get_event_loop().time()
        elapsed = now - self._last_request
        if elapsed < REQUEST_DELAY_SECS:
            await asyncio.sleep(REQUEST_DELAY_SECS - elapsed)
        self._last_request = asyncio.get_event_loop().time()

    async def get_json(self, path: str, params: dict | None = None) -> dict[str, Any]:
        """GET con rate limit y reintentos."""
        for attempt in range(MAX_RETRIES):
            await self._throttle()
            try:
                r = await self._client.get(path, params=params)
                if r.status_code in (429, 503):
                    wait = 2 ** (attempt + 1)
                    logger.warning("HTTP %d en %s, retry en %ds", r.status_code, path, wait)
                    await asyncio.sleep(wait)
                    continue
                r.raise_for_status()
                return r.json()
            except httpx.HTTPError as e:
                if attempt == MAX_RETRIES - 1:
                    raise
                logger.warning("Error %s en %s, retry %d", e, path, attempt + 1)
                await asyncio.sleep(2 ** (attempt + 1))
        raise RuntimeError(f"Max retries en {path}")

    async def get_signed_url(self, archivo_id: str) -> str:
        """Obtiene URL firmada S3 para descargar PDF."""
        data = await self.get_json("actas/file", params={"id": archivo_id})
        if not data.get("success") or not data.get("data"):
            raise ValueError(f"No se obtuvo URL firmada para {archivo_id}")
        return data["data"]

    async def download_pdf(self, signed_url: str) -> bytes:
        """Descarga PDF desde URL firmada S3."""
        await self._throttle()
        r = await self._client.get(signed_url)
        r.raise_for_status()
        if "application/pdf" not in r.headers.get("content-type", ""):
            logger.warning("Content-type inesperado: %s", r.headers.get("content-type"))
        return r.content

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()
