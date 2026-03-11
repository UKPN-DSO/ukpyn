"""Test that tutorial notebooks are valid and can execute.

These tests verify that tutorial notebooks:
1. Are syntactically valid JSON
2. Have valid notebook structure
3. Can execute without errors (when API key is available)

Note: Execution tests require UKPN_API_KEY environment variable.
Tests requiring API access are marked with @pytest.mark.integration.
"""

import json
from pathlib import Path

import pytest

# Find tutorials directory relative to this test file
TUTORIALS_DIR = Path(__file__).parent.parent / "tutorials"


def get_tutorial_notebooks() -> list[Path]:
    """Get all tutorial notebook files."""
    if not TUTORIALS_DIR.exists():
        return []
    return sorted(TUTORIALS_DIR.glob("*.ipynb"))


def get_notebook_ids() -> list[str]:
    """Get notebook names for test parameterization."""
    return [nb.stem for nb in get_tutorial_notebooks()]


class TestTutorialNotebooksValid:
    """Test that tutorial notebooks are valid."""

    @pytest.mark.parametrize(
        "notebook_path",
        get_tutorial_notebooks(),
        ids=get_notebook_ids(),
    )
    def test_notebook_is_valid_json(self, notebook_path: Path) -> None:
        """Verify notebook file is valid JSON."""
        content = notebook_path.read_text(encoding="utf-8")
        # Should not raise
        data = json.loads(content)
        assert isinstance(data, dict)

    @pytest.mark.parametrize(
        "notebook_path",
        get_tutorial_notebooks(),
        ids=get_notebook_ids(),
    )
    def test_notebook_has_required_keys(self, notebook_path: Path) -> None:
        """Verify notebook has required Jupyter notebook structure."""
        content = notebook_path.read_text(encoding="utf-8")
        data = json.loads(content)

        # Required top-level keys for Jupyter notebook format
        assert "cells" in data, f"{notebook_path.name} missing 'cells' key"
        assert "metadata" in data, f"{notebook_path.name} missing 'metadata' key"
        assert "nbformat" in data, f"{notebook_path.name} missing 'nbformat' key"

    @pytest.mark.parametrize(
        "notebook_path",
        get_tutorial_notebooks(),
        ids=get_notebook_ids(),
    )
    def test_notebook_cells_are_valid(self, notebook_path: Path) -> None:
        """Verify each cell has required structure."""
        content = notebook_path.read_text(encoding="utf-8")
        data = json.loads(content)

        for i, cell in enumerate(data.get("cells", [])):
            assert "cell_type" in cell, (
                f"{notebook_path.name} cell {i} missing 'cell_type'"
            )
            assert "source" in cell, f"{notebook_path.name} cell {i} missing 'source'"
            assert cell["cell_type"] in ("code", "markdown", "raw"), (
                f"{notebook_path.name} cell {i} has invalid cell_type: "
                f"{cell['cell_type']}"
            )


class TestTutorialNotebooksExecute:
    """Test that tutorial notebooks can execute.

    These tests require:
    - UKPN_API_KEY environment variable set
    - Network access to the UKPN Open Data Portal

    Run with: pytest -m integration
    """

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "notebook_path",
        get_tutorial_notebooks(),
        ids=get_notebook_ids(),
    )
    def test_notebook_executes(
        self,
        notebook_path: Path,
        api_key_available: bool,
        run_slow_notebook_tests: bool,
    ) -> None:
        """Execute notebook and verify no errors.

        This test uses nbconvert to execute the notebook.
        Requires jupyter and nbconvert to be installed.
        """
        if not run_slow_notebook_tests:
            pytest.skip(
                "Slow notebook execution tests are disabled by default. "
                "Set UKPYN_RUN_SLOW_NOTEBOOK_TESTS=1 to enable."
            )

        if not api_key_available:
            pytest.fail(
                "UKPN_API_KEY is required for integration tests. "
                "Set it in your environment or .env file, then rerun with: "
                "pytest -m integration"
            )

        try:
            import nbformat
            from nbconvert.preprocessors import ExecutePreprocessor
        except ImportError:
            pytest.skip(
                "nbconvert not installed - install with: pip install nbconvert jupyter"
            )

        # Read the notebook
        with open(notebook_path, encoding="utf-8") as f:
            nb = nbformat.read(f, as_version=4)

        # Create executor with timeout
        ep = ExecutePreprocessor(
            timeout=300,  # 5 minute timeout per cell
            kernel_name="python3",
        )

        # Execute the notebook
        # This will raise an exception if any cell fails
        try:
            ep.preprocess(nb, {"metadata": {"path": str(TUTORIALS_DIR)}})
        except Exception as e:
            pytest.fail(f"Notebook {notebook_path.name} failed to execute: {e}")


class TestTutorialNotebooksSyntax:
    """Test that code in tutorial notebooks has valid Python syntax."""

    @pytest.mark.parametrize(
        "notebook_path",
        get_tutorial_notebooks(),
        ids=get_notebook_ids(),
    )
    def test_notebook_code_syntax(self, notebook_path: Path) -> None:
        """Verify all code cells have valid Python syntax.

        Note: Jupyter notebooks support top-level await, which is not valid
        in regular Python. Cells with async code are wrapped in an async
        function for syntax validation.
        """
        content = notebook_path.read_text(encoding="utf-8")
        data = json.loads(content)

        for i, cell in enumerate(data.get("cells", [])):
            if cell.get("cell_type") != "code":
                continue

            source = cell.get("source", [])
            code = "".join(source) if isinstance(source, list) else source

            # Skip empty cells
            if not code.strip():
                continue

            # Skip cells with magic commands (like %matplotlib)
            if code.strip().startswith("%") or code.strip().startswith("!"):
                continue

            # If cell contains await/async with, wrap in async function
            # (Jupyter supports top-level await, regular Python doesn't)
            if "await " in code or "async with" in code or "async for" in code:
                # Wrap in async function for syntax check
                indented_code = "\n".join(f"    {line}" for line in code.split("\n"))
                code = f"async def _notebook_cell():\n{indented_code}"

            try:
                compile(code, f"{notebook_path.name}:cell_{i}", "exec")
            except SyntaxError as e:
                pytest.fail(f"Syntax error in {notebook_path.name} cell {i}: {e}")
