"""
World Bank Open Data API Client
Handles all API interactions with retry logic and pagination
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


class WBAPIClient:
    """Client for World Bank Open Data API v2."""

    BASE_URL = "https://api.worldbank.org/v2"
    DEFAULT_TIMEOUT = 30
    MAX_RETRIES = 3
    RETRY_DELAY = 2  # seconds
    MAX_PER_PAGE = 20000

    def __init__(
        self,
        timeout: int = DEFAULT_TIMEOUT,
        base_url: str | None = None,
        max_retries: int | None = None,
        retry_delay: int | None = None,
    ):
        """Initialize the API client with a persistent session.

        Args:
            timeout:     request timeout in seconds.
            base_url:    override WB API base URL (default: class constant).
            max_retries: override retry attempt count (default: class constant).
            retry_delay: override retry backoff base in seconds (default: class constant).

        Per-instance overrides let update_metadata.py honour
        config_update.yaml's wb_api.{base_url,retry_count,retry_delay}
        keys (previously inert — _make_request read class constants).
        """
        self.timeout = timeout
        self.base_url = base_url if base_url else self.BASE_URL
        self.max_retries = max_retries if max_retries is not None else self.MAX_RETRIES
        self.retry_delay = retry_delay if retry_delay is not None else self.RETRY_DELAY
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "wbopendata-metadata-updater/1.0",
                "Accept": "application/json",
            }
        )

    def __enter__(self) -> "WBAPIClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def close(self) -> None:
        self.session.close()

    def fetch_indicators(self, per_page: int = 10000) -> List[Dict]:
        """Fetch all indicators from WB API with pagination."""
        logger.info("Fetching indicators from WB API...")

        per_page = max(1, min(per_page, self.MAX_PER_PAGE))
        indicators: List[Dict] = []
        page = 1
        total_pages = None

        while True:
            url = f"{self.base_url}/indicators"
            params = {
                "format": "json",
                "per_page": per_page,
                "page": page,
            }

            logger.info("Fetching page %s/%s...", page, total_pages or "?")
            data = self._make_request(url, params)

            if not data or len(data) < 2:
                raise ValueError(f"Unexpected API response format for indicators (page {page})")

            metadata = data[0]
            records = data[1]

            if total_pages is None:
                # WB v2 API sometimes returns "pages"/"total" as strings; coerce
                # so `page >= total_pages` can't raise TypeError downstream.
                # max(1, ...) guards against 0/None/'' or string '0' yielding
                # an int < 1 that would immediately exit the pagination loop.
                try:
                    total_pages = max(1, int(metadata.get("pages") or 1))
                except (TypeError, ValueError):
                    total_pages = 1
                try:
                    total_indicators = max(0, int(metadata.get("total") or 0))
                except (TypeError, ValueError):
                    total_indicators = 0
                logger.info("Total indicators: %s, Pages: %s", total_indicators, total_pages)

            indicators.extend(records)

            if page >= total_pages:
                break

            page += 1
            time.sleep(self.retry_delay)

        logger.info("Fetched %s indicators", len(indicators))
        return indicators

    def fetch_sources(self) -> List[Dict]:
        """Fetch all data sources."""
        logger.info("Fetching sources from WB API...")

        url = f"{self.base_url}/sources"
        params = {"format": "json", "per_page": 100}
        data = self._make_request(url, params)

        if not data or len(data) < 2:
            raise ValueError("Unexpected API response format for sources")

        sources = data[1]
        logger.info("Fetched %s sources", len(sources))
        return sources

    def fetch_topics(self) -> List[Dict]:
        """Fetch all topics."""
        logger.info("Fetching topics from WB API...")

        url = f"{self.base_url}/topics"
        params = {"format": "json", "per_page": 100}
        data = self._make_request(url, params)

        if not data or len(data) < 2:
            raise ValueError("Unexpected API response format for topics")

        topics = data[1]
        logger.info("Fetched %s topics", len(topics))
        return topics

    def fetch_indicator_metadata(self, code: str) -> Optional[Dict]:
        """Fetch one indicator's raw metadata from the WB API.

        Returns the first record from /v2/indicator/{code} or None if the
        API returned no records. Unlike fetch_indicators() (which is
        paginated bulk), this is a single targeted lookup intended for
        wb_discovery.describe() — the live counterpart to info() (which
        reads from the YAML cache).
        """
        url = f"{self.base_url}/indicator/{code}"
        params = {"format": "json"}
        data = self._make_request(url, params)
        if not data or len(data) < 2 or not data[1]:
            return None
        return data[1][0]

    def _make_request(self, url: str, params: Dict[str, Any]) -> Any:
        """Make HTTP request with retry logic.

        Floor max_retries at 1 attempt so config `retry_count=0` means
        "one attempt, no retries" (the conventional interpretation),
        not "zero HTTP attempts" (which would silently no-op every call).
        """
        last_error: Exception | None = None
        for attempt in range(max(1, self.max_retries)):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.Timeout as exc:
                last_error = exc
                logger.warning(
                    "Timeout on attempt %s/%s for %s; retrying in %ss",
                    attempt + 1,
                    self.max_retries,
                    url,
                    self.retry_delay * (attempt + 1),
                )
            except requests.exceptions.RequestException as exc:
                last_error = exc
                logger.warning(
                    "Request failed on attempt %s/%s for %s: %s",
                    attempt + 1,
                    self.max_retries,
                    url,
                    exc,
                )

            if attempt < self.max_retries - 1:
                time.sleep(self.retry_delay * (attempt + 1))

        raise RuntimeError(f"API request failed after {self.max_retries} attempts: {url}") from last_error

    def save_raw_data(self, data: Dict[str, List], output_dir: Path = Path("data/raw")) -> None:
        """Save raw API responses to JSON files."""
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        for data_type, records in data.items():
            output_file = output_dir / f"{data_type}_{timestamp}.json"

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=2, ensure_ascii=False)

            logger.info("Saved %s %s to %s", len(records), data_type, output_file)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    with WBAPIClient() as client:
        indicators = client.fetch_indicators(per_page=5000)
        sources = client.fetch_sources()
        topics = client.fetch_topics()

        print("\nFetched:")
        print(f"  - {len(indicators)} indicators")
        print(f"  - {len(sources)} sources")
        print(f"  - {len(topics)} topics")

        client.save_raw_data({"indicators": indicators, "sources": sources, "topics": topics})
