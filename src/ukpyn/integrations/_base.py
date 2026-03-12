"""Base interface for third-party integrations.

All contrib integrations should inherit from BaseContrib to ensure
consistent interface and lazy dependency loading.
"""

from abc import ABC, abstractmethod
from typing import Any

from ..spatial import Bounds


class BaseContrib(ABC):
    """Base class for third-party data integrations.

    Subclasses must implement:
        - _check_deps(): Verify required dependencies are installed
        - get_for_bounds(): Get data within geographic bounds

    Example implementation:
        class WeatherContrib(BaseContrib):
            def _check_deps(self) -> None:
                try:
                    import earthkit.data
                except ImportError:
                    raise ImportError(
                        "Weather integration requires earthkit-data.\\n"
                        "Install with: pip install ukpyn[weather]"
                    )

            def get_for_bounds(self, bounds, **kwargs):
                import earthkit.data as ekd
                # Fetch weather data...
    """

    def __init__(self) -> None:
        """Initialize the integration, checking dependencies."""
        self._check_deps()

    @abstractmethod
    def _check_deps(self) -> None:
        """Check that required dependencies are installed.

        Should raise ImportError with helpful message if deps missing.
        """
        pass

    @abstractmethod
    def get_for_bounds(
        self,
        bounds: Bounds,
        **kwargs: Any,
    ) -> Any:
        """Get data within the specified geographic bounds.

        Args:
            bounds: Geographic bounding box (north, south, east, west)
            **kwargs: Integration-specific parameters

        Returns:
            Integration-specific data format
        """
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable name of this integration."""
        pass

    @property
    @abstractmethod
    def install_hint(self) -> str:
        """Installation instructions for missing dependencies."""
        pass
