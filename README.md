# Eesti Äriregistri CLI / Estonian Business Registry CLI

[Eestikeelne versioon](#eesti-äriregistri-cli) | [English version](#estonian-business-registry-cli-1)

---

# Eesti Äriregistri CLI

Kiire, põhjalik ja visuaalselt selge tööriist Eesti äriregistri avaandmete allalaadimiseks, ühendamiseks, otsimiseks ja rikastamiseks.

## Funktsioonid

- **Paralleelsed ja jätkatavad allalaadimised**: Laeb alla kõik registrifailid korraga, kasutades HTTP Range päiseid katkenud allalaadimiste jätkamiseks.
- **SQLite arhitektuur (vaikimisi)**: Kasutab kohalikku SQLite andmebaasi kiireteks otsinguteks, keerukateks filtriteks ja andmete rikastamiseks.
- **Kakskeelne tugi**: Toetus nii eesti- kui ka ingliskeelsetele käskudele ja väljundile. Vaikimisi keel on eesti keel.
- **Inkrementaalne ja jätkatav andmeühendus**: Töötleb faile kirje haaval koos oleku salvestamisega. Katkestuse korral jätkab sealt, kus pooleli jäi.
- **Põhjalikud toimikud**: Kuvab kõik registrist kättesaadavad andmed, sh nime/aadressi ajalugu, kapitali muudatused ja tehnilised märkused.
- **Ilus terminali kasutajaliides**: Kasutab `rich` teeki vormindatud tabelite, ajaloo puude ja süntaksi esiletõstmisega JSON-i jaoks.
- **Isikukoodide avalikustamine (rikastamine)**: Parsib automaatselt ametlikke registrikaardi PDF-e, et avalikustada isikukoodid, mis on avaandmete failides peidetud (hashitud).
- **Täpsem otsing**: Otsi nime, asukoha (linn/maakond), staatuse või isiku (nimi/isikukood) järgi.
- **Sektsioonide filtreerimine**: Vaata toimiku konkreetseid osi (nt ainult omandisuhted või personal), kasutades spetsiaalseid lippe.

## Kasutamine

### 1. Andmete sünkroonimine
```bash
uv run registry.py sünk
```

### 2. Otsing
```bash
uv run registry.py otsi "Sunyata"
```

#### Sektsioonide filtreerimine
```bash
# Vaata ainult omandisuhteid ja tegelikke kasusaajaid
uv run registry.py otsi "Sunyata" --ownership --beneficiaries
```

### 3. Rikastamine
```bash
uv run registry.py rikasta 16631240
```

---

# Estonian Business Registry CLI

A high-performance, exhaustive, and beautiful tool for downloading, merging, searching, and enriching the Estonian Business Registry open data.

## Features

- **Parallel & Resumable Downloads**: Fetches all registry files simultaneously using HTTP Range headers to resume interrupted downloads.
- **SQLite Architecture (Default)**: Uses a local SQLite database for instant searches, complex filtering, and reliable data enrichment.
- **Dual-Language Support**: Support for both Estonian and English commands and output. Default is Estonian.
- **Incremental & Resumable Merge**: Processes files record-by-record with checkpointing. If interrupted, it picks up exactly where it left off.
- **Exhaustive Dossiers**: Displays every piece of data from the registry, including name/address history, capital changes, and technical annotations.
- **Beautiful Terminal UI**: Powered by `rich`, featuring formatted tables, trees for history, and syntax-highlighted JSON.
- **ID Unmasking (Enrichment)**: Automatically parses official Registry Card PDFs to unmask personal ID codes that are hashed in the bulk open data.
- **Advanced Search**: Filter by term, location (city/county), status, or persons (Name/ID).
- **Section Filtering**: View specific parts of a dossier (e.g., just ownership or personnel) using dedicated CLI flags.

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

The tool supports commands in both Estonian and English.

### 1. Synchronize Data
Downloads updates and merges them incrementally into the local database:
```bash
uv run registry.py sync    # English
uv run registry.py sünk   # Estonian
```

### 2. Search
Display a complete dossier for a company:
```bash
uv run registry.py search "Sunyata"   # Defaults to English output
uv run registry.py otsi "Sunyata"    # Defaults to Estonian output
```

#### Language Overrides
You can force the language regardless of the command used:
```bash
uv run registry.py otsi "Sunyata" --en   # Estonian command, English output
uv run registry.py search "Sunyata" --ee  # English command, Estonian output
```

#### Filtering by Section
Use section flags to isolate specific data points:
```bash
# See only ownership and beneficial owners
uv run registry.py search "Sunyata" --ownership --beneficiaries

# See only historical records (names, addresses, capital)
uv run registry.py search "Sunyata" --history
```

#### Advanced Search Filters
```bash
# Search for a person across all companies
uv run registry.py search -p "Tony Benoy"

# Filter by location and status
uv run registry.py search -l "Harju" -s "Registrisse kantud"

# Output full raw JSON with syntax highlighting
uv run registry.py search 16631240 --json
```

### 3. Enrichment (ID Unmasking)
Unmask personal ID codes by downloading the official PDF:
```bash
# Unmask IDs for a specific company
uv run registry.py enrich 16631240    # English
uv run registry.py rikasta 16631240   # Estonian
```

### 4. Exporting
Export the entire database to a JSON file:
```bash
uv run registry.py export dump.json
uv run registry.py ekspordi dump.json
```

## Multi-Backend Architecture
The tool is designed to be database-agnostic. It uses an abstract `RegistryBackend` interface, allowing for easy expansion to other databases (e.g., PostgreSQL, MongoDB). 

To add a new backend:
1. Subclass `RegistryBackend` in `registry.py`.
2. Implement the abstract methods (search, insert, etc.).
3. Pass your new backend instance to the `EstonianRegistry` constructor.

## Available Sections
- `--core`: Technical metadata and IDs.
- `--general`: Registry-wide attributes and flags.
- `--history`: Chronological logs of names, addresses, and capital.
- `--personnel`: Board members and signing rights.
- `--ownership`: Shareholders and share pledges.
- `--beneficiaries`: Ultimate beneficial owners.
- `--operations`: EMTAK activities, reports, and contacts.
- `--registry`: The complete chronological registry card log.
- `--enrichment`: Data extracted from live PDF cards.

## License
MIT

## Disclaimer / Hoiatus

**IMPORTANT: READ CAREFULLY.**

This software is provided "as is" and "with all faults," without any warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose, and non-infringement.

1.  **Compliance with RIK Terms:** By using this tool, you agree to comply with the [Terms of Use of the Estonian Business Registry (RIK) Open Data](https://ariregister.rik.ee/eng/open_data). You are solely responsible for ensuring that your use of the data (including automated downloading and parsing) adheres to their policies and rate limits.
2.  **No Liability:** In no event shall the authors, contributors, or copyright holders be liable for any claim, damages, or other liability, whether in an action of contract, tort, or otherwise, arising from, out of, or in connection with the software or the use or other dealings in the software.
3.  **Data Accuracy:** This tool retrieves and parses public data from the Estonian Business Registry (RIK). The authors make no guarantees regarding the accuracy, completeness, or timeliness of the data provided by the registry or the parsing logic within this tool.
4.  **Use at Your Own Risk:** Users are solely responsible for any decisions made or actions taken based on the information provided by this tool. This software is not intended for use in high-stakes environments, legal compliance verification, or financial due diligence without independent verification.
5.  **No Official Affiliation:** This project is an independent tool and is not affiliated with, endorsed by, or sponsored by the Centre of Registers and Information Systems (RIK) or any Estonian government entity.

By using this software, you acknowledge that you have read this disclaimer, agree to its terms, and accept full responsibility for your use of the retrieved data.
