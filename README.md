# Eesti Äriregistri CLI / Estonian Business Registry CLI

[Eestikeelne versioon](#eesti-äriregistri-cli) | [English version](#estonian-business-registry-cli-1)

---

# Eesti Äriregistri CLI

Kiire, põhjalik ja visuaalselt selge tööriist Eesti äriregistri avaandmete allalaadimiseks, ühendamiseks, otsimiseks, analüüsimiseks ja rikastamiseks. Mõeldud nii arendajatele kui ka ärikasutajatele.

## Funktsioonid

- **Paralleelsed ja jätkatavad allalaadimised**: Laeb alla kõik registrifailid korraga, kasutades HTTP Range päiseid katkenud allalaadimiste jätkamiseks.
- **SQLite arhitektuur**: Kasutab kohalikku SQLite andmebaasi kiireteks otsinguteks, keerukateks filtriteks ja andmete rikastamiseks.
- **Kakskeelne tugi**: Toetus nii eesti- kui ka ingliskeelsetele käskudele ja väljundile.
- **Ärikasutajate otsing (`leia`)**: Otsi ettevõtteid tegevusala, asukoha, töötajate arvu, kapitali, kontaktide ja asutamiskuupäeva järgi.
- **Isikuotsing (`isik`)**: Otsi juhatuse liikmeid, osanikke ja kasusaajaid üle kõigi ettevõtete. Näita isiku võrgustikku.
- **Kontsernide kaardistamine (`kontsern`)**: Rekursiivne omandiahela kaardistamine — kes omab keda, tütarettevõtted.
- **Äriaruanded (`aruanne`)**: Valmis tururaportid: turuülevaade, uued ettevõtted, tegevusalade edetabel, piirkondlik analüüs, pankrotid, töötajate trend.
- **Andmeanalüüs (`analüüs`)**: Grupeeri maakonna, staatuse, õigusliku vormi, EMTAK koodi, asutamisaasta, kapitalivahemiku, töötajate vahemiku, rolli või riigi järgi.
- **CSV eksport**: Ekspordi filtreeritud tulemused Exceli-sõbralikku CSV-faili koos kontaktide, kapitali, KMKR numbri ja tegevusalaga.
- **Tegevusalade nimekaart**: 70+ tegevusala ingliskeelne nimetus → EMTAK koodid. Kirjavigade soovitused kaasa arvatud.
- **Põhjalikud toimikud**: Kuvab kõik registrist kättesaadavad andmed, sh nime/aadressi ajalugu, kapitali muudatused ja tehnilised märkused.
- **Isikukoodide avalikustamine (rikastamine)**: Parsib ametlikke registrikaardi PDF-e, et avalikustada isikukoodid.
- **Voogesitlusel põhinev JSON-i parser**: Töötleb 4+ GB JSON-faile mälutõhusalt (`ijson`).

## Kasutamine

### 1. Andmete sünkroonimine
```bash
uv run registry.py sünk
```

### 2. Ettevõtete leidmine
```bash
# Leia tarkvaraettevõtted Tallinnas, vähemalt 10 töötajat
uv run registry.py leia --industry software --location Tallinn --min-employees 10

# Leia ehitusettevõtted kapitaliga üle 25 000 €
uv run registry.py leia --industry construction --min-capital 25000

# Leia ettevõtted e-posti ja veebilehega
uv run registry.py leia --industry software --has-email --has-website

# Leia kasvavad ettevõtted (töötajate arv suureneb)
uv run registry.py leia --industry software --growing --limit 20

# Leia konkreetne ettevõte nimega
uv run registry.py leia "Bolt"
```

### 3. Isikuotsing
```bash
# Otsi isikut nime järgi
uv run registry.py isik "Markus Villig"

# Näita isiku võrgustikku (kõik seotud ettevõtted)
uv run registry.py isik "Markus Villig" --network

# Otsi rolli järgi
uv run registry.py isik --role "Juhatuse liige" --limit 20
```

### 4. Kontsernide kaardistamine
```bash
# Omandiahel mõlemas suunas
uv run registry.py kontsern 14532901

# Ainult omanikud (üles)
uv run registry.py kontsern 14532901 --direction up

# Ainult tütarettevõtted (alla)
uv run registry.py kontsern 14532901 --direction down --depth 3
```

### 5. Äriandmete raportid
```bash
uv run registry.py aruanne market-overview
uv run registry.py aruanne new-companies --period 2024
uv run registry.py aruanne top-industries --location Tartu
uv run registry.py aruanne regional --county "Harju maakond"
uv run registry.py aruanne employee-trend --code 14532901
uv run registry.py aruanne employee-trend --industry software
```

### 6. Analüüs
```bash
uv run registry.py analüüs --by county --industry software
uv run registry.py analüüs --by capital-range --industry software
uv run registry.py analüüs --by employee-range
uv run registry.py analüüs --by role
uv run registry.py analüüs --by country
```

### 7. Eksport
```bash
uv run registry.py ekspordi ettevotted.csv --industry software --location Tallinn
uv run registry.py ekspordi ettevotted.csv --has-email --min-capital 2500 --limit 100
uv run registry.py ekspordi koik.json --limit 1000
```

### 8. Otsing (detailne toimik)
```bash
uv run registry.py otsi "Sunyata" --ownership --beneficiaries
```

### 9. Rikastamine
```bash
uv run registry.py rikasta 16631240
```

---

# Estonian Business Registry CLI

A business intelligence tool for the Estonian Business Registry. Download, search, analyze, and export data on 366,000+ Estonian companies. Built for both developers and business users.

## Features

- **Parallel & Resumable Downloads**: Fetches all registry files simultaneously using HTTP Range headers to resume interrupted downloads.
- **SQLite Architecture**: Uses a local SQLite database for instant searches, complex filtering, and data enrichment.
- **Dual-Language Support**: All commands and output available in both Estonian and English.
- **Business-Friendly Search (`find`)**: Search companies by industry, location, employee count, capital, contacts, and founding date.
- **Person Search (`person`)**: Search board members, shareholders, and beneficiaries across all 366K companies. View a person's full network of company affiliations.
- **Corporate Group Mapping (`group`)**: Recursive ownership chain mapping — find who owns a company (up), what subsidiaries it has (down), or both.
- **Pre-Built Reports (`report`)**: One-command business intelligence: market overview, new companies, top industries, regional analysis, bankruptcies, employee trends.
- **Data Analysis (`analyze`)**: Group companies by county, status, legal form, EMTAK code, founding year, capital range, employee range, person role, or beneficiary country.
- **CSV Export**: Export filtered results to Excel-ready CSV with contacts, capital, VAT number, employee counts, and industry data.
- **Contact & Capital Filters**: Filter by `--has-email`, `--has-website`, `--min-capital`, `--max-capital`, and `--growing` (rising headcount).
- **Industry Name Mapping**: 70+ plain English industry names mapped to EMTAK codes. Includes typo suggestions.
- **Exhaustive Dossiers**: Displays every piece of data from the registry, including name/address history, capital changes, and technical annotations.
- **Beautiful Terminal UI**: Powered by `rich`, featuring formatted tables, trees for history, and syntax-highlighted JSON.
- **ID Unmasking (Enrichment)**: Parses official Registry Card PDFs to unmask personal ID codes that are hashed in the bulk open data.
- **Streaming JSON Parser**: Handles 4+ GB JSON files memory-efficiently using `ijson`.

## Prerequisites

- **Python 3.12+**
- **uv**: Recommended for dependency management.
- **jq** (Optional): Speeds up JSON processing during the merge step.

## Installation

```bash
git clone https://github.com/your-repo/estonia-registry.git
cd estonia-registry
uv sync
```

## Quick Start

```bash
# 1. Download and merge registry data (~10 min first time)
uv run registry.py sync

# 2. Rebuild derived columns & person index (one-time, after first sync)
uv run registry.py merge --force

# 3. Find software companies in Tallinn with 10+ employees
uv run registry.py --en find --industry software --location Tallinn --min-employees 10

# 4. Search for a person across all companies
uv run registry.py --en person "Markus Villig" --network

# 5. Get a market overview report
uv run registry.py --en report market-overview

# 6. Export construction companies to CSV
uv run registry.py --en export construction.csv --industry construction
```

## Usage

The tool supports commands in both Estonian and English. Use `--en` to force English output, `--ee` for Estonian.

### Synchronize Data
Downloads and merges all registry files into the local database:
```bash
uv run registry.py sync
uv run registry.py merge --force   # Re-process all files + rebuild derived columns & person index
```

### Find Companies (Business Search)
The `find` command is designed for non-technical users. It returns a compact summary table:
```bash
# By industry (plain English names)
uv run registry.py --en find --industry software --location Tallinn
uv run registry.py --en find --industry "real estate" --founded-after 2023-01-01
uv run registry.py --en find --industry restaurant --min-employees 5

# Capital filters
uv run registry.py --en find --industry software --min-capital 25000 --limit 10
uv run registry.py --en find --min-capital 1000000    # Companies with 1M+ capital

# Contact filters
uv run registry.py --en find --industry software --has-email --has-website --limit 10

# Growing companies (employee count increasing year-over-year)
uv run registry.py --en find --industry software --growing --limit 20

# By name or registry code
uv run registry.py --en find "Bolt"
uv run registry.py --en find 14532901

# Show full dossier instead of summary table
uv run registry.py --en find --industry healthcare --location Tartu --full

# Export results directly to CSV
uv run registry.py --en find --industry construction --csv construction.csv
```

Available `--industry` names include: `software`, `construction`, `restaurant`, `real estate`, `finance`, `healthcare`, `manufacturing`, `retail`, `transport`, `agriculture`, `energy`, `media`, and 60+ more. Use `--list-industries` to see all:
```bash
uv run registry.py --list-industries --en
```

### Person Search
Search board members, shareholders, and beneficiaries across all companies:
```bash
# Search by name
uv run registry.py --en person "Markus Villig"

# Search by ID code
uv run registry.py --en person --id 14056464

# Show full network (all companies a person is connected to)
uv run registry.py --en person "Markus Villig" --network

# Filter by role or source
uv run registry.py --en person --role "Juhatuse liige" --limit 20
uv run registry.py --en person --source shareholder --code 14532901
```

### Corporate Group Mapping
Map ownership chains and corporate structures:
```bash
# Full ownership map (shareholders + subsidiaries)
uv run registry.py --en group 14532901

# Only shareholders (who owns this company)
uv run registry.py --en group 14532901 --direction up

# Only subsidiaries (what does this company own)
uv run registry.py --en group 14532901 --direction down --depth 3
```

### Reports (Business Intelligence)
Pre-built reports that combine multiple analyses into one output:
```bash
# Full market snapshot: totals, top counties, top industries, founding trends
uv run registry.py --en report market-overview

# New companies in a given year: top industries, locations, legal forms
uv run registry.py --en report new-companies --period 2024

# Top industries in a specific location
uv run registry.py --en report top-industries --location Tartu

# Growth of a specific industry over time
uv run registry.py --en report industry-growth --industry software

# Regional deep-dive for a county
uv run registry.py --en report regional --county "Harju maakond"

# Bankruptcies and liquidations
uv run registry.py --en report bankruptcies --period 2024

# Employee trends (single company or industry-wide)
uv run registry.py --en report employee-trend --code 14532901
uv run registry.py --en report employee-trend --industry software
```

### Analyze
Group and count companies by dimension, with optional filters:
```bash
# Top counties for software companies
uv run registry.py --en analyze --by county --industry software

# Status breakdown for companies in Tartu
uv run registry.py --en analyze --by status --location Tartu

# EMTAK industry breakdown
uv run registry.py --en analyze --by emtak --top 20

# Founding trend by year
uv run registry.py --en analyze --by year --founded-after 2010-01-01

# Legal form distribution
uv run registry.py --en analyze --by legal-form

# Capital distribution
uv run registry.py --en analyze --by capital-range
uv run registry.py --en analyze --by capital-range --industry software

# Employee size distribution
uv run registry.py --en analyze --by employee-range --industry software

# Person roles across companies
uv run registry.py --en analyze --by role

# Beneficiary countries
uv run registry.py --en analyze --by country
```

### Search (Detailed Dossiers)
The original detailed search that shows full company dossiers:
```bash
uv run registry.py search "Sunyata"

# Filter by section
uv run registry.py search "Sunyata" --ownership --beneficiaries --personnel

# Search by person
uv run registry.py search -p "Tony Benoy"

# Filter by EMTAK code, date, legal form
uv run registry.py search --emtak 62 --founded-after 2024-01-01 --legal-form "Aktsiaselts" --limit 3

# Output as JSON
uv run registry.py search 16631240 --json
```

### Export
Export filtered company data to CSV or JSON:
```bash
# CSV with flattened columns (Excel-ready, UTF-8 BOM)
uv run registry.py --en export software_tallinn.csv --industry software --location Tallinn

# With capital and contact filters
uv run registry.py --en export leads.csv --industry software --has-email --min-capital 2500 --limit 100

# JSON export
uv run registry.py --en export all.json --limit 1000

# With employee filtering
uv run registry.py --en export big_companies.csv --min-employees 100
```

CSV columns: `code, name, status, county, city, legal_form, founded, main_industry_code, main_industry_name, employees, capital, capital_currency, vat_number, email, phone, website`

### Statistics
Quick overview of the database, including coverage of all data dimensions:
```bash
uv run registry.py --en stats
```

### Enrichment (ID Unmasking)
Unmask personal ID codes by downloading the official PDF:
```bash
uv run registry.py enrich 16631240
```

## Language Overrides
```bash
uv run registry.py otsi "Sunyata" --en   # Estonian command, English output
uv run registry.py search "Sunyata" --ee  # English command, Estonian output
```

## Available Dossier Sections
- `--core`: Technical metadata and IDs.
- `--general`: Registry-wide attributes and flags.
- `--history`: Chronological logs of names, addresses, and capital.
- `--personnel`: Board members and signing rights.
- `--ownership`: Shareholders and share pledges.
- `--beneficiaries`: Ultimate beneficial owners.
- `--operations`: EMTAK activities, reports, and contacts.
- `--registry`: The complete chronological registry card log.
- `--enrichment`: Data extracted from live PDF cards.

## Architecture
The tool uses an abstract `RegistryBackend` interface, allowing for expansion to other databases (e.g., PostgreSQL). To add a new backend, subclass `RegistryBackend` in `registry.py` and implement the abstract methods.

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
