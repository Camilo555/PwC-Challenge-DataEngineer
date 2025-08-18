"""Base API client with retry logic and error handling."""

import asyncio
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import aiohttp
from aiohttp import ClientTimeout

from de_challenge.core.logging import get_logger

logger = get_logger(__name__)


class BaseAPIClient(ABC):
    """Base class for external API clients with common functionality."""

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3,
        rate_limit_per_second: float = 1.0,
    ) -> None:
        """
        Initialize base API client.

        Args:
            base_url: Base URL for the API
            api_key: Optional API key for authentication
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            rate_limit_per_second: Rate limit (requests per second)
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = ClientTimeout(total=timeout)
        self.max_retries = max_retries
        self.rate_limit_delay = 1.0 / rate_limit_per_second
        self._last_request_time = 0.0
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            headers = {"User-Agent": "PwC-DataEngineer-Challenge/1.0"}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            
            self._session = aiohttp.ClientSession(
                headers=headers,
                timeout=self.timeout
            )
        return self._session

    async def _enforce_rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        current_time = time.time()
        time_since_last = current_time - self._last_request_time
        
        if time_since_last < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last
            await asyncio.sleep(sleep_time)
        
        self._last_request_time = time.time()

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        retry_count: int = 0,
    ) -> Dict[str, Any]:
        """
        Make HTTP request with retry logic.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            params: Query parameters
            data: Request body data
            retry_count: Current retry attempt

        Returns:
            API response as dictionary

        Raises:
            aiohttp.ClientError: If request fails after all retries
        """
        await self._enforce_rate_limit()
        
        session = await self._get_session()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        try:
            async with session.request(
                method=method,
                url=url,
                params=params,
                json=data
            ) as response:
                response.raise_for_status()
                return await response.json()
                
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if retry_count < self.max_retries:
                wait_time = 2 ** retry_count  # Exponential backoff
                logger.warning(
                    f"Request failed (attempt {retry_count + 1}/{self.max_retries + 1}), "
                    f"retrying in {wait_time}s: {e}"
                )
                await asyncio.sleep(wait_time)
                return await self._make_request(method, endpoint, params, data, retry_count + 1)
            else:
                logger.error(f"Request failed after {self.max_retries + 1} attempts: {e}")
                raise

    async def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make GET request."""
        return await self._make_request("GET", endpoint, params=params)

    async def post(
        self,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make POST request."""
        return await self._make_request("POST", endpoint, params=params, data=data)

    async def close(self) -> None:
        """Close HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    @abstractmethod
    async def health_check(self) -> bool:
        """Check if the API is healthy and accessible."""
        pass