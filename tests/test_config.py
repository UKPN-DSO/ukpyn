"""Tests for configuration management."""

import pytest

from ukpyn.config import API_KEY_ENV_VAR, BASE_URL, Config, check_api_key
from ukpyn.exceptions import AuthenticationError


def test_config_defaults(monkeypatch) -> None:
    """Config uses package defaults when not overridden."""
    monkeypatch.delenv(API_KEY_ENV_VAR, raising=False)
    monkeypatch.setattr("ukpyn.config.load_dotenv", lambda: None)

    config = Config()

    assert config.api_key is None
    assert config.base_url == BASE_URL
    assert config.timeout == 30
    assert config.api_url.endswith("/api/explore/v2.1")
    assert config.has_api_key is False


def test_config_reads_api_key_from_environment(monkeypatch) -> None:
    """Config loads API key from environment variable."""
    monkeypatch.setenv(API_KEY_ENV_VAR, "env-key")

    config = Config()

    assert config.api_key == "env-key"
    assert config.has_api_key is True


def test_config_init_api_key_overrides_environment(monkeypatch) -> None:
    """Explicit api_key takes precedence over environment."""
    monkeypatch.setenv(API_KEY_ENV_VAR, "env-key")

    config = Config(api_key="explicit-key")

    assert config.api_key == "explicit-key"


def test_config_base_url_trailing_slash_removed() -> None:
    """Base URL is normalized by removing trailing slash."""
    config = Config(base_url="https://example.com/")

    assert config.base_url == "https://example.com"
    assert config.api_url == "https://example.com/api/explore/v2.1"


def test_headers_without_api_key(monkeypatch) -> None:
    """Headers omit Authorization when key is not configured."""
    monkeypatch.delenv(API_KEY_ENV_VAR, raising=False)
    monkeypatch.setattr("ukpyn.config.load_dotenv", lambda: None)
    config = Config()

    headers = config.get_headers()

    assert headers["Accept"] == "application/json"
    assert headers["Content-Type"] == "application/json"
    assert "Authorization" not in headers


def test_headers_with_api_key() -> None:
    """Headers include Authorization with configured API key."""
    config = Config(api_key="test-key")

    headers = config.get_headers()

    assert headers["Authorization"] == "Apikey test-key"


def test_empty_api_key_not_considered_configured(monkeypatch) -> None:
    """Empty API key should not be considered configured."""
    monkeypatch.delenv(API_KEY_ENV_VAR, raising=False)
    monkeypatch.setattr("ukpyn.config.load_dotenv", lambda: None)
    config = Config(api_key="")

    assert config.has_api_key is False
    assert "Authorization" not in config.get_headers()


def test_check_api_key_raises_when_missing(monkeypatch) -> None:
    """check_api_key raises AuthenticationError when key is not set."""
    monkeypatch.delenv(API_KEY_ENV_VAR, raising=False)
    monkeypatch.setattr("ukpyn.config.load_dotenv", lambda: None)

    with pytest.raises(AuthenticationError, match="UKPN_API_KEY"):
        check_api_key()


def test_check_api_key_passes_when_set(monkeypatch) -> None:
    """check_api_key does not raise when key is set."""
    monkeypatch.setenv(API_KEY_ENV_VAR, "test-key")

    check_api_key()  # should not raise


def test_config_reads_api_key_from_dotenv(monkeypatch) -> None:
    """Config loads API key from a local .env before building headers."""
    monkeypatch.delenv(API_KEY_ENV_VAR, raising=False)

    def fake_load_dotenv() -> None:
        monkeypatch.setenv(API_KEY_ENV_VAR, "dotenv-key")

    monkeypatch.setattr("ukpyn.config.load_dotenv", fake_load_dotenv)

    config = Config()

    assert config.api_key == "dotenv-key"
    assert config.has_api_key is True
    assert config.get_headers()["Authorization"] == "Apikey dotenv-key"
