"""Custom exceptions for the UKPN API client."""


class UKPNError(Exception):
    """Base exception for UKPN API errors."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.status_code:
            return f"[{self.status_code}] {self.message}"
        return self.message


class AuthenticationError(UKPNError):
    """Raised when API authentication fails."""

    def __init__(
        self, message: str = "Authentication failed. Check your API key."
    ) -> None:
        super().__init__(message, status_code=401)


class RateLimitError(UKPNError):
    """Raised when API rate limit is exceeded."""

    def __init__(
        self,
        message: str = "Rate limit exceeded. Please wait before making more requests.",
        retry_after: int | None = None,
    ) -> None:
        super().__init__(message, status_code=429)
        self.retry_after = retry_after


class NotFoundError(UKPNError):
    """Raised when a requested resource is not found."""

    def __init__(self, message: str = "The requested resource was not found.") -> None:
        super().__init__(message, status_code=404)


class ValidationError(UKPNError):
    """Raised when request validation fails."""

    def __init__(self, message: str = "Request validation failed.") -> None:
        super().__init__(message, status_code=400)


class UnrecognisedFieldError(ValidationError):
    """Raised when the API rejects a query due to an unknown field name.

    This typically means the dataset schema has changed and the installed
    version of *ukpyn* does not yet know about the new (or renamed) field.
    Upgrading the package resolves this in most cases.
    """

    def __init__(
        self,
        message: str = "Request referenced an unrecognised field.",
        *,
        fields: list[str] | None = None,
    ) -> None:
        from importlib.metadata import version

        try:
            current = version("ukpyn")
        except Exception:
            current = "unknown"

        hint = (
            f" (ukpyn=={current}). "
            "A newer release may support this field — try: "
            "pip install --upgrade ukpyn"
        )
        full_message = message + hint
        super().__init__(full_message)
        self.fields = fields or []


class ServerError(UKPNError):
    """Raised when the API server returns an error."""

    def __init__(self, message: str = "The API server encountered an error.") -> None:
        super().__init__(message, status_code=500)
