# Estonian Business Registry CLI

A high-performance, memory-efficient tool for downloading, merging, searching, and enriching the Estonian Business Registry open data.

## Features

- **Parallel & Incremental Downloads**: Fetches all registry files simultaneously. Skips files that haven't changed on the server.
- **SQLite Support (Optional)**: Migrate to a local database for 100x faster searches and more reliable enrichment.
- **Streaming Merge**: Processes large JSON files using streaming I/O to maintain a low memory footprint.
- **Advanced Search**: Filter by name, registry code, location (city/county), status, or even persons (ID/Name).
- **PDF Enrichment**: Automatically downloads and parses official Registry Card PDFs to extract management board details and personal IDs.
- **Exporting**: Save search results to JSON or CSV for external analysis.

## Prerequisites

- **Python 3.12+**
- **uv**: Recommended for dependency management.
- **jq** (Optional): Dramatically reduces RAM usage during the merge process.

## Installation

```bash
git clone https://github.com/your-repo/estonia-registry.git
cd estonia-registry
uv sync
```

## Usage

### 1. Synchronize Data
Downloads updates (if available) and merges them:
```bash
uv run registry.py sync
```

### 2. Search & Export
```bash
# Search for a person across all companies
uv run registry.py search -p "Tony Benoy"

# Filter by location and status, then export to CSV
uv run registry.py search -l "Harju" -s "Registrisse kantud" --export harju_companies.csv

# Search by code (instant jump via index)
uv run registry.py search -c 16631240

# Translate results to English
uv run registry.py search -n "Bolt" --translate
```

### 3. Enrichment
```bash
# Enrich specific companies (limit 10 per run)
uv run registry.py enrich 16631240
```

### 4. Statistics
```bash
uv run registry.py stats
```

## SDK Usage

You can also use the registry logic directly in your own Python code:

```python
from registry import EstonianRegistry

# Initialize the registry (defaults to 'data' directory)
reg = EstonianRegistry()

# Search for a company
results = reg.search(term="Bolt", translate=True)

for company in results:
    print(f"{company['name']} ({company['registry_code']})")
    print(f"Status: {company['status']}")

# Get analytics
stats = reg.get_analytics()
print(f"Total companies: {stats['total']}")
```

## Database Backend (Optional)
For much faster performance, you can use SQLite:
1. Initialize the database during merge:
   ```bash
   uv run registry.py merge --use-db
   ```
2. Subsequent commands will automatically detect `registry.db` and use it for instant searches.

## Data Structure
The tool creates a `chunks/` directory:
- `chunk_XXX.json`: Compressed company records.
- `manifest.json`: Index for instant lookups by registry code.

## License
MIT
