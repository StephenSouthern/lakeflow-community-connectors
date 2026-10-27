"""HTTP client for the DWP Stat-Xplore Open Data API.

Official contract (https://stat-xplore.dwp.gov.uk/webapi/online-help/Open-Data-API.html):

* Service root ``https://stat-xplore.dwp.gov.uk/webapi/rest/v1``.
* Four endpoints: ``GET /schema[/{id}]``, ``POST /table``, ``GET /info``,
  ``GET /rate_limit``.
* Authentication: a static API key in the ``APIKey`` request header.
* Rate limit: 2,000 requests per hour per key, reported on every response via
  ``X-RateLimit``, ``X-RateLimit-Remaining`` and ``X-RateLimit-Reset`` (UNIX
  epoch milliseconds). ``/rate_limit`` itself does not count against it.
* ``/schema`` responses with more than 100 children are paginated through a
  ``Link: <...>; rel="next"`` header.

Reliability behaviour implemented here:

* A single serial request stream per client (no parallelism) with a minimum
  interval between request starts.
* Bounded retries for 408/429/500/502/503/504 and network failures, honouring
  ``Retry-After`` (seconds or HTTP-date) and otherwise using exponential
  backoff with jitter. Every sleep is capped by ``retry_max_delay_seconds``.
* Pre-emptive back-off when ``X-RateLimit-Remaining`` falls to the configured
  reserve: wait for the reset when it is close, otherwise fail clearly rather
  than burn the remaining hourly budget.
* 400/401/403/404/422 are terminal and never retried.
* The API key is sent only to the configured HTTPS origin, is never logged,
  and is scrubbed from every error message.
"""

import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Optional
from urllib.parse import quote, urlsplit

import requests

STAT_XPLORE_DEFAULT_BASE_URL = "https://stat-xplore.dwp.gov.uk/webapi/rest/v1"
STAT_XPLORE_RETRIABLE_STATUS = frozenset({408, 429, 500, 502, 503, 504})
_SX_MAX_SCHEMA_PAGES = 500
_SX_ERROR_SNIPPET_CHARS = 300


class StatXploreError(RuntimeError):
    """Base error. Messages never contain the API key."""

    def __init__(self, message: str, status_code: Optional[int] = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class StatXploreAuthError(StatXploreError):
    """HTTP 401/403 - invalid key or no access to the requested object."""


class StatXploreRateLimitError(StatXploreError):
    """Rate limit still active after bounded retries, or budget exhausted."""


class StatXploreResponseError(StatXploreError):
    """The response was malformed, incomplete or exceeded a safety limit."""


def stat_xplore_parse_retry_after(
    value: Optional[str], now: Optional[datetime] = None
) -> Optional[float]:
    """Parse ``Retry-After`` (delta-seconds or HTTP-date) into seconds >= 0."""
    if value is None or not str(value).strip():
        return None
    raw = str(value).strip()
    try:
        return max(0.0, float(raw))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(raw)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    return max(0.0, (when - current).total_seconds())


def _sx_header(headers: Any, name: str) -> Optional[str]:
    if not headers:
        return None
    wanted = name.lower()
    for key, value in dict(headers).items():
        if str(key).lower() == wanted:
            return str(value)
    return None


def _sx_int_or_none(value: Optional[str]) -> Optional[int]:
    try:
        return int(float(value)) if value is not None else None
    except (TypeError, ValueError):
        return None


class StatXploreClient:  # pylint: disable=too-many-instance-attributes
    """Serial, paced, retrying client for one Stat-Xplore API key."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        api_key: str,
        base_url: str = STAT_XPLORE_DEFAULT_BASE_URL,
        *,
        timeout_seconds: float = 120.0,
        min_interval_seconds: float = 2.0,
        max_retries: int = 5,
        retry_max_delay_seconds: float = 60.0,
        rate_limit_reserve: int = 5,
        rate_limit_max_wait_seconds: float = 300.0,
        accept_language: Optional[str] = None,
        session: Any = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
        rng: Callable[[], float] = random.random,
    ) -> None:
        if not api_key or not str(api_key).strip():
            raise ValueError("Stat-Xplore 'api_key' connection option is required.")
        parsed = urlsplit(str(base_url or "").strip())
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "Stat-Xplore 'base_url' must be an https:// service root without "
                "credentials, query string or fragment."
            )
        if timeout_seconds <= 0 or min_interval_seconds < 0 or max_retries < 0:
            raise ValueError("timeout must be > 0; pacing and retries must be >= 0.")
        if retry_max_delay_seconds < 0 or rate_limit_reserve < 0:
            raise ValueError("retry_max_delay_seconds and rate_limit_reserve must be >= 0.")
        self.base_url = str(base_url).strip().rstrip("/")
        self._origin = (parsed.scheme, (parsed.netloc or "").lower())
        self._api_key = str(api_key).strip()
        self.timeout_seconds = float(timeout_seconds)
        self.min_interval_seconds = float(min_interval_seconds)
        self.max_retries = int(max_retries)
        self.retry_max_delay_seconds = float(retry_max_delay_seconds)
        self.rate_limit_reserve = int(rate_limit_reserve)
        self.rate_limit_max_wait_seconds = float(rate_limit_max_wait_seconds)
        self.accept_language = accept_language or None
        self._session = session
        self._sleep = sleep
        self._clock = clock
        self._wall_clock = wall_clock
        self._rng = rng
        self._next_request_at = 0.0
        self.rate_limit_remaining: Optional[int] = None
        self.rate_limit_reset_ms: Optional[int] = None
        self.stats = {"requests": 0, "retries": 0, "responses_429": 0, "table_requests": 0}

    def __repr__(self) -> str:  # never expose the key
        return f"StatXploreClient(base_url={self.base_url!r})"

    # ------------------------------------------------------------------ helpers

    @property
    def session(self):
        if self._session is None:
            self._session = requests.Session()
        return self._session

    def _headers(self, with_body: bool) -> dict:
        headers = {"APIKey": self._api_key, "Accept": "application/json"}
        if with_body:
            headers["Content-Type"] = "application/json"
        if self.accept_language:
            headers["Accept-Language"] = self.accept_language
        return headers

    def _sanitize(self, text: Any) -> str:
        out = str(text if text is not None else "")
        if self._api_key:
            out = out.replace(self._api_key, "***")
        return out[:_SX_ERROR_SNIPPET_CHARS]

    def _check_origin(self, url: str) -> None:
        parsed = urlsplit(url)
        if (parsed.scheme, (parsed.netloc or "").lower()) != self._origin:
            raise StatXploreResponseError(
                "Refusing to send the API key to a different origin than base_url "
                f"({parsed.scheme}://{parsed.netloc})."
            )

    def _backoff(self, attempt: int) -> float:
        base = min(self.retry_max_delay_seconds, float(2 ** attempt))
        return min(self.retry_max_delay_seconds, base * (0.75 + 0.5 * self._rng()))

    def _pace(self) -> None:
        wait = self._next_request_at - self._clock()
        if wait > 0:
            self._sleep(wait)
        self._next_request_at = self._clock() + self.min_interval_seconds

    def _respect_budget(self) -> None:
        remaining = self.rate_limit_remaining
        if remaining is None or remaining > self.rate_limit_reserve:
            return
        reset_ms = self.rate_limit_reset_ms
        wait = (reset_ms / 1000.0 - self._wall_clock()) if reset_ms else None
        if wait is not None and wait <= 0:
            self.rate_limit_remaining = None
            return
        if wait is not None and wait <= self.rate_limit_max_wait_seconds:
            self._sleep(wait)
            self.rate_limit_remaining = None
            return
        raise StatXploreRateLimitError(
            f"Stat-Xplore hourly request budget is down to {remaining} (reserve "
            f"{self.rate_limit_reserve}); the limit resets at epoch ms {reset_ms}. "
            "No checkpoint was advanced - rerun after the reset.",
            status_code=429,
        )

    def _record_rate_headers(self, headers: Any) -> None:
        remaining = _sx_int_or_none(_sx_header(headers, "X-RateLimit-Remaining"))
        reset = _sx_int_or_none(_sx_header(headers, "X-RateLimit-Reset"))
        if remaining is not None:
            self.rate_limit_remaining = remaining
        if reset is not None:
            self.rate_limit_reset_ms = reset

    def _error_detail(self, response: Any) -> str:
        try:
            body = response.json()
        except ValueError:
            return ""
        if isinstance(body, dict):
            parts = [str(body.get(k)) for k in ("errorType", "message") if body.get(k)]
            return self._sanitize(": ".join(parts))
        return ""

    # ------------------------------------------------------------------ core

    def request_json(
        self, method: str, url: str, body: Optional[dict] = None
    ) -> tuple[Any, Any]:
        """Issue one logical request with bounded retries. Returns (json, response)."""
        self._check_origin(url)
        path = urlsplit(url).path
        label = f"{method} {path}"
        for attempt in range(self.max_retries + 1):
            self._respect_budget()
            self._pace()
            self.stats["requests"] += 1
            try:
                response = self.session.request(
                    method,
                    url,
                    headers=self._headers(body is not None),
                    json=body,
                    timeout=self.timeout_seconds,
                )
            except (requests.ConnectionError, requests.Timeout,
                    requests.exceptions.ChunkedEncodingError) as exc:
                if attempt >= self.max_retries:
                    raise StatXploreError(
                        f"Stat-Xplore {label}: network failure after {attempt + 1} "
                        f"attempt(s) ({type(exc).__name__})."
                    ) from None
                self.stats["retries"] += 1
                self._sleep(self._backoff(attempt))
                continue

            self._record_rate_headers(response.headers)
            status = int(response.status_code)
            if status in STAT_XPLORE_RETRIABLE_STATUS:
                if status == 429:
                    self.stats["responses_429"] += 1
                if attempt >= self.max_retries:
                    cls = StatXploreRateLimitError if status == 429 else StatXploreError
                    raise cls(
                        f"Stat-Xplore {label}: HTTP {status} persisted after "
                        f"{attempt + 1} attempt(s). {self._error_detail(response)}".strip(),
                        status_code=status,
                    )
                delay = stat_xplore_parse_retry_after(_sx_header(response.headers, "Retry-After"))
                if delay is None:
                    delay = self._backoff(attempt)
                self.stats["retries"] += 1
                self._sleep(min(delay, self.retry_max_delay_seconds))
                continue
            if status in (401, 403):
                raise StatXploreAuthError(
                    f"Stat-Xplore {label}: HTTP {status} - API key rejected or no access. "
                    f"{self._error_detail(response)}".strip(),
                    status_code=status,
                )
            if status >= 400:
                raise StatXploreError(
                    f"Stat-Xplore {label}: HTTP {status}. {self._error_detail(response)}".strip(),
                    status_code=status,
                )
            try:
                data = response.json()
            except ValueError:
                raise StatXploreResponseError(
                    f"Stat-Xplore {label}: HTTP {status} response was not valid JSON.",
                    status_code=status,
                ) from None
            return data, response
        raise StatXploreError(f"Stat-Xplore {label}: retries exhausted.")  # pragma: no cover

    def _get_object(self, url: str) -> tuple[dict, Any]:
        data, response = self.request_json("GET", url)
        if not isinstance(data, dict):
            raise StatXploreResponseError(f"Stat-Xplore GET {urlsplit(url).path}: expected a JSON object.")
        return data, response

    # ------------------------------------------------------------------ endpoints

    def schema_url(self, item_id: Optional[str] = None) -> str:
        if not item_id:
            return f"{self.base_url}/schema"
        return f"{self.base_url}/schema/{quote(str(item_id), safe='')}"

    def get_schema(self, item_id: Optional[str] = None) -> dict:
        """GET /schema[/{id}], following ``Link rel=next`` pagination of children."""
        url = self.schema_url(item_id)
        first, response = self._get_object(url)
        children = list(first.get("children") or [])
        pages = 1
        next_url = (getattr(response, "links", None) or {}).get("next", {}).get("url")
        while next_url:
            pages += 1
            if pages > _SX_MAX_SCHEMA_PAGES:
                raise StatXploreResponseError(
                    f"Stat-Xplore schema item {item_id!r} exceeded {_SX_MAX_SCHEMA_PAGES} pages."
                )
            page, response = self._get_object(next_url)
            page_children = page.get("children")
            if not isinstance(page_children, list):
                raise StatXploreResponseError(
                    f"Stat-Xplore schema item {item_id!r}: paginated page without 'children'."
                )
            children.extend(page_children)
            next_url = (getattr(response, "links", None) or {}).get("next", {}).get("url")
        if "children" in first or pages > 1:
            first = dict(first)
            first["children"] = children
        return first

    def get_info(self) -> dict:
        return self._get_object(f"{self.base_url}/info")[0]

    def get_rate_limit(self) -> dict:
        return self._get_object(f"{self.base_url}/rate_limit")[0]

    def post_table(self, query: dict) -> dict:
        """POST /table. Returns the decoded response object."""
        self.stats["table_requests"] += 1
        data, _ = self.request_json("POST", f"{self.base_url}/table", body=query)
        if not isinstance(data, dict):
            raise StatXploreResponseError("Stat-Xplore POST /table: expected a JSON object.")
        return data
