"""Tests for custom exception types."""

from ukpyn.exceptions import (
    AuthenticationError,
    NotFoundError,
    RateLimitError,
    ServerError,
    UKPNError,
    ValidationError,
)


def test_ukpn_error_string_without_status_code() -> None:
    """Base error string omits status prefix when status_code is None."""
    error = UKPNError("plain error")

    assert error.status_code is None
    assert str(error) == "plain error"


def test_ukpn_error_string_with_status_code() -> None:
    """Base error string includes status prefix when available."""
    error = UKPNError("request failed", status_code=503)

    assert str(error) == "[503] request failed"


def test_authentication_error_defaults() -> None:
    """AuthenticationError sets 401 status and default message."""
    error = AuthenticationError()

    assert error.status_code == 401
    assert "Authentication failed" in str(error)


def test_not_found_error_defaults() -> None:
    """NotFoundError sets 404 status and default message."""
    error = NotFoundError()

    assert error.status_code == 404
    assert "not found" in str(error).lower()


def test_validation_error_defaults() -> None:
    """ValidationError sets 400 status and default message."""
    error = ValidationError()

    assert error.status_code == 400
    assert "validation failed" in str(error).lower()


def test_server_error_defaults() -> None:
    """ServerError sets 500 status and default message."""
    error = ServerError()

    assert error.status_code == 500
    assert "server" in str(error).lower()


def test_rate_limit_error_defaults_and_retry_after() -> None:
    """RateLimitError stores retry_after and uses 429 status."""
    error = RateLimitError(retry_after=30)

    assert error.status_code == 429
    assert error.retry_after == 30
    assert "rate limit exceeded" in str(error).lower()


def test_rate_limit_error_custom_message() -> None:
    """RateLimitError accepts custom message."""
    error = RateLimitError("Custom limit message")

    assert str(error) == "[429] Custom limit message"
