"""Third-party data integrations for ukpyn (optional extras).

This module provides optional integrations with external data sources.
Each integration requires additional dependencies that are not installed by default.

Available integrations:
    - weather: Weather data via earthkit-data (install: pip install ukpyn[weather])
    - energy_market: Energy market data via Elexon (install: pip install ukpyn[energy-market])

Usage:
    # Import will raise helpful error if deps not installed
    from ukpyn.integrations import weather

    # Or check availability first
    from ukpyn.integrations import is_available
    if is_available("weather"):
        from ukpyn.integrations import weather
"""

# Lazy import pattern - modules are imported on first access
# This avoids ImportError until user actually tries to use them

_AVAILABLE_INTEGRATIONS = {
    "weather": "ukpyn.integrations.weather",
    "energy_market": "ukpyn.integrations.energy_market",
}


def is_available(integration: str) -> bool:
    """
    Check if an integration's dependencies are installed.

    Args:
        integration: Name of the integration ('weather', 'energy_market')

    Returns:
        True if the integration can be imported, False otherwise

    Example:
        from ukpyn.integrations import is_available

        if is_available("weather"):
            from ukpyn.integrations import weather
            data = weather.get_for_bounds(bounds)
    """
    if integration not in _AVAILABLE_INTEGRATIONS:
        return False

    try:
        if integration == "weather":
            import earthkit.data  # noqa: F401

            return True
        elif integration == "energy_market":
            # Check for elexon deps when implemented
            return False  # Not yet implemented
    except ImportError:
        return False

    return False


def list_integrations() -> list[str]:
    """
    List all available third-party integrations.

    Returns:
        List of integration names
    """
    return list(_AVAILABLE_INTEGRATIONS.keys())


__all__ = ["is_available", "list_integrations"]
