"""Configuration management for the UK Power Networks API client."""

import os

from dotenv import load_dotenv

# API Base URL constant
BASE_URL: str = "https://ukpowernetworks.opendatasoft.com"

# API version
API_VERSION: str = "2.1"

# Default timeout in seconds
DEFAULT_TIMEOUT: int = 30

# Environment variable name for API key
API_KEY_ENV_VAR: str = "UKPN_API_KEY"


def load_environment() -> None:
    """Load variables from a local .env file into the process environment."""
    load_dotenv()


def check_api_key() -> None:
    """Verify that an API key is available.

    Checks the ``UKPN_API_KEY`` environment variable (loading any ``.env``
    file first via *python-dotenv* when available) and raises
    :class:`~ukpyn.exceptions.AuthenticationError` with actionable guidance
    if the key is missing.

    Call this at the top of scripts or notebooks for an immediate, clear
    error instead of waiting for a 401 response from the API.

    Raises:
        AuthenticationError: If no API key is found.
    """
    load_environment()

    if not os.getenv(API_KEY_ENV_VAR):
        from .exceptions import AuthenticationError

        raise AuthenticationError(
            f"{API_KEY_ENV_VAR} environment variable is not set. "
            "Set it to your UK Power Networks Open Data API key.\n"
            "  export UKPN_API_KEY='your-key-here'  # Linux / macOS\n"
            "  $env:UKPN_API_KEY='your-key-here'    # PowerShell\n"
            "Or add it to a .env file in your project root."
        )


class Config:
    """Configuration class for UK Power Networks API client settings."""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = BASE_URL,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> None:
        """
        Initialize configuration.

        Args:
            api_key: API key for authentication. If not provided, will attempt
                     to load from UKPN_API_KEY environment variable.
            base_url: Base URL for the API. Defaults to UK Power Networks OpenDataSoft URL.
            timeout: Request timeout in seconds. Defaults to 30.
        """
        load_environment()
        self._api_key = api_key or os.getenv(API_KEY_ENV_VAR)
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    @property
    def api_key(self) -> str | None:
        """Get the API key."""
        return self._api_key

    @property
    def base_url(self) -> str:
        """Get the base URL."""
        return self._base_url

    @property
    def api_url(self) -> str:
        """Get the full API URL including version."""
        return f"{self._base_url}/api/explore/v{API_VERSION}"

    @property
    def timeout(self) -> int:
        """Get the timeout value."""
        return self._timeout

    @property
    def has_api_key(self) -> bool:
        """Check if an API key is configured."""
        return self._api_key is not None and len(self._api_key) > 0

    def get_headers(self) -> dict[str, str]:
        """
        Get HTTP headers for API requests.

        Returns:
            Dictionary of headers including Authorization if API key is set.
        """
        headers: dict[str, str] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if self.has_api_key:
            headers["Authorization"] = f"Apikey {self._api_key}"
        return headers
