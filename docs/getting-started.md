# Getting Started

This page takes you from zero setup to your first successful ukpyn API call.

If you are a beginner, follow this page top to bottom without skipping steps.

Need help with terminology? See the [Glossary](glossary.md).

## 1) Install ukpyn

For most users:

```bash
pip install ukpyn
```

If you cloned this repository and want all optional tools (for notebooks and
development workflows):

```bash
pip install -e ".[all]"
```

Shell note: use double quotes for extras in PowerShell and bash. In Windows
CMD, prefer `pip install -e .[all]`.

## 2) Verify your Python environment

Confirm your terminal is using the environment where you installed ukpyn:

```bash
python --version
pip show ukpyn
```

You should see Python 3.11+ and package details for `ukpyn`.

## 3) Configure your API key

ukpyn reads your key from the `UKPN_API_KEY` environment variable.

### Option A (quick test in current shell)

PowerShell:

```powershell
$env:UKPN_API_KEY="your_api_key_here"
```

macOS/Linux shell:

```bash
export UKPN_API_KEY=your_api_key_here
```

Windows CMD:

```bat
set UKPN_API_KEY=your_api_key_here
```

### Option B (recommended for local projects)

Create a `.env` file in your project root:

```bash
UKPN_API_KEY=your_api_key_here
```

## 4) Make your first API call

Create a file called `first_call.py`:

```python
from ukpyn import ltds

table_3a = ltds.get_table_3a(licence_area="EPN")

print(f"Total records (server count): {table_3a.total_count}")
print(f"Records in this response: {len(table_3a.records)}")
```

Run it:

```bash
python first_call.py
```

If the command prints counts without error, your setup is working.

## 5) Understand the response object

Most data-fetching calls return a structured response with fields like:

- `total_count`: how many records match on the server
- `records`: list of returned record objects
- `links`: pagination or API metadata links (if present)

To inspect a single record safely:

```python
if table_3a.records:
	first = table_3a.records[0]
	print(first.id)
	print(first.fields)
```

## 6) Try one simple filter change

The fastest way to learn is to modify one parameter and rerun:

```python
from ukpyn import ltds

epn = ltds.get_table_3a(licence_area="EPN")
spn = ltds.get_table_3a(licence_area="SPN")

print("EPN:", epn.total_count)
print("SPN:", spn.total_count)
```

## Notebook kernel alignment

If you are using notebooks, make sure the notebook kernel is the same Python
environment where `ukpyn` is installed.

Register a kernel (once):

```bash
python -m ipykernel install --user --name ukpyn --display-name "Python (ukpyn)"
```

Then select **Python (ukpyn)** in VS Code / Jupyter before running cells.

## Common beginner issues

### `ModuleNotFoundError: No module named 'ukpyn'`

- Cause: package installed in a different environment
- Fix: activate the intended environment, reinstall `ukpyn`, rerun script

### Authentication / API key errors

- Cause: missing or invalid `UKPN_API_KEY`
- Fix: set key again in current shell, then rerun command in same shell

### Notebook works differently from terminal

- Cause: kernel mismatch
- Fix: switch notebook kernel to the environment where `ukpyn` is installed

## Next steps

- Continue to [Tutorials](tutorials.md) for guided notebook workflows
- Use [Orchestrators](orchestrators.md) to choose the right domain module
- Return to this page whenever you need a clean setup checklist
