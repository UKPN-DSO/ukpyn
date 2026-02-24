"""Tests for optional integration modules."""

from __future__ import annotations

from typing import Any

import pytest

import ukpyn.integrations as integrations_module
from ukpyn.integrations import _base, list_integrations
from ukpyn.integrations.weather import WeatherContrib, get_for_bounds
from ukpyn.spatial import Bounds


class _DemoContrib(_base.BaseContrib):
    """Minimal concrete contrib used to test BaseContrib behavior."""

    def __init__(self) -> None:
        self.checked = False
        super().__init__()

    def _check_deps(self) -> None:
        self.checked = True

    def get_for_bounds(self, bounds: Bounds, **kwargs: Any) -> dict[str, Any]:
        return {"bounds": bounds, "kwargs": kwargs}

    @property
    def name(self) -> str:
        return "demo"

    @property
    def install_hint(self) -> str:
        return "pip install demo"


def test_base_contrib_calls_check_deps() -> None:
    """BaseContrib constructor invokes dependency check."""
    contrib = _DemoContrib()
    assert contrib.checked is True


def test_base_contrib_concrete_properties_and_method() -> None:
    """Concrete contrib subclass exposes required API."""
    contrib = _DemoContrib()

    assert contrib.name == "demo"
    assert contrib.install_hint == "pip install demo"
    assert contrib.get_for_bounds({"north": 1, "south": 0, "east": 1, "west": 0})[
        "bounds"
    ]["north"] == 1


def test_list_integrations_contains_expected_names() -> None:
    """Integration registry includes weather and energy market entries."""
    names = list_integrations()

    assert "weather" in names
    assert "energy_market" in names


@pytest.mark.parametrize("name", ["unknown", "not-real"])
def test_is_available_unknown_integration_returns_false(name: str) -> None:
    """Unknown integration names are reported unavailable."""
    assert integrations_module.is_available(name) is False


def test_is_available_energy_market_returns_false() -> None:
    """Energy market integration is currently a placeholder."""
    assert integrations_module.is_available("energy_market") is False


def test_is_available_weather_importerror_branch(monkeypatch) -> None:
    """Weather availability returns False when earthkit import fails."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "earthkit.data":
            raise ImportError("missing")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert integrations_module.is_available("weather") is False


def test_is_available_weather_true_branch(monkeypatch) -> None:
    """Weather availability returns True when earthkit import succeeds."""
    import builtins

    real_import = builtins.__import__

    class DummyEarthkitData:  # noqa: D401
        """Dummy module object for import interception."""

    def fake_import(name, *args, **kwargs):
        if name == "earthkit.data":
            return DummyEarthkitData
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert integrations_module.is_available("weather") is True


def test_weather_contrib_metadata(monkeypatch) -> None:
    """WeatherContrib exposes metadata without importing earthkit when cached."""
    from ukpyn.integrations import weather

    monkeypatch.setattr(weather, "_ekd", object())
    contrib = WeatherContrib()

    assert "Weather" in contrib.name
    assert "ukpyn[weather]" in contrib.install_hint


def test_weather_get_for_bounds_not_implemented(monkeypatch) -> None:
    """Weather integration currently raises NotImplementedError with context."""
    from ukpyn.integrations import weather

    monkeypatch.setattr(weather, "_ekd", object())
    contrib = WeatherContrib()

    with pytest.raises(NotImplementedError) as exc:
        contrib.get_for_bounds(
            bounds={"north": 51.6, "south": 51.4, "east": 0.1, "west": -0.2},
            variables=["temperature"],
        )

    assert "under development" in str(exc.value)
    assert "temperature" in str(exc.value)


def test_weather_get_for_bounds_defaults_variables(monkeypatch) -> None:
    """Weather get_for_bounds uses default variable list when none provided."""
    from ukpyn.integrations import weather

    monkeypatch.setattr(weather, "_ekd", object())
    contrib = WeatherContrib()

    with pytest.raises(NotImplementedError) as exc:
        contrib.get_for_bounds(
            bounds={"north": 51.6, "south": 51.4, "east": 0.1, "west": -0.2},
            variables=None,
        )

    message = str(exc.value)
    assert "2m_temperature" in message
    assert "10m_u_component_of_wind" in message


def test_weather_module_level_get_for_bounds_delegates(monkeypatch) -> None:
    """Module-level helper delegates to WeatherContrib."""
    from ukpyn.integrations import weather

    monkeypatch.setattr(weather, "_ekd", object())

    called = {}

    def fake_method(_self, **kwargs):
        called.update(kwargs)
        return "done"

    monkeypatch.setattr(WeatherContrib, "get_for_bounds", fake_method)

    result = get_for_bounds(
        bounds={"north": 1.0, "south": 0.0, "east": 1.0, "west": 0.0},
        variables=["wind_speed"],
    )

    assert result == "done"
    assert called["variables"] == ["wind_speed"]


def test_weather_check_earthkit_importerror_message(monkeypatch) -> None:
    """_check_earthkit should raise a helpful ImportError when dependency is missing."""
    import builtins
    from ukpyn.integrations import weather

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "earthkit.data":
            raise ImportError("missing dependency")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(weather, "_ekd", None)
    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(ImportError) as exc:
        weather._check_earthkit()

    assert "requires earthkit-data" in str(exc.value)


class _CallsBaseContribMethods(_base.BaseContrib):
    """Test-only class that executes BaseContrib abstract method bodies via super()."""

    def _check_deps(self) -> None:
        return super()._check_deps()

    def get_for_bounds(self, bounds: Bounds, **kwargs: Any) -> Any:
        return super().get_for_bounds(bounds=bounds, **kwargs)

    @property
    def name(self) -> str:
        return super().name

    @property
    def install_hint(self) -> str:
        return super().install_hint


def test_base_contrib_abstract_bodies_return_none_when_called_via_super() -> None:
    """Calling abstract method/property bodies via super should be safe no-ops."""
    contrib = _CallsBaseContribMethods()

    assert contrib.get_for_bounds({"north": 1, "south": 0, "east": 1, "west": 0}) is None
    assert contrib.name is None
    assert contrib.install_hint is None


def test_is_available_known_but_unhandled_name_falls_back_false(monkeypatch) -> None:
    """Known-but-unhandled integration names should use final fallback False path."""
    monkeypatch.setitem(
        integrations_module._AVAILABLE_INTEGRATIONS,
        "custom_integration",
        "ukpyn.integrations.custom",
    )
    assert integrations_module.is_available("custom_integration") is False


def test_weather_check_earthkit_sets_cached_module_on_success(monkeypatch) -> None:
    """_check_earthkit should cache imported module object when import succeeds."""
    import sys
    import types
    from ukpyn.integrations import weather

    earthkit_pkg = types.ModuleType("earthkit")
    data_mod = types.ModuleType("earthkit.data")
    earthkit_pkg.data = data_mod

    monkeypatch.setattr(weather, "_ekd", None)
    monkeypatch.setitem(sys.modules, "earthkit", earthkit_pkg)
    monkeypatch.setitem(sys.modules, "earthkit.data", data_mod)

    weather._check_earthkit()
    assert weather._ekd is data_mod
