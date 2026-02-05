#!/usr/bin/env python3
"""
Estonian Registry CLI

A high-performance tool for downloading, merging, and searching 
Estonian Business Registry open data.
"""

import argparse
import csv
import json
import mmap
import os
import shutil
import subprocess
import time
import urllib.request
import zipfile
import io
import re
import requests
import sqlite3
from pathlib import Path
from threading import Thread, Lock
from pypdf import PdfReader
from collections import defaultdict
from datetime import datetime

# ============================================================
# Database Logic (Optional)
# ============================================================

class RegistryDB:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self):
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    code INTEGER PRIMARY KEY,
                    name TEXT,
                    status TEXT,
                    maakond TEXT,
                    linn TEXT,
                    full_data JSON,
                    enrichment JSON
                )
            """)
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_name ON companies(name)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON companies(status)")

    def insert_batch(self, batch):
        with self.conn:
            self.conn.executemany(
                "INSERT OR REPLACE INTO companies (code, name, status, maakond, linn, full_data) VALUES (?, ?, ?, ?, ?, ?)",
                [(
                    item.get('ariregistri_kood'),
                    item.get('nimi'),
                    item.get('staatus'),
                    item.get('aadress_maakond'),
                    item.get('aadress_linn'),
                    json.dumps(item)
                ) for item in batch]
            )

    def update_enrichment(self, code: int, enrichment: dict):
        with self.conn:
            self.conn.execute(
                "UPDATE companies SET enrichment = ? WHERE code = ?",
                (json.dumps(enrichment), code)
            )

    def search(self, term=None, person=None, location=None, status=None):
        query = "SELECT * FROM companies WHERE 1=1"
        params = []
        
        if term:
            if term.isdigit():
                query += " AND code = ?"
                params.append(int(term))
            else:
                query += " AND name LIKE ?"
                params.append(f"%{term}%")
        
        if location:
            query += " AND (maakond LIKE ? OR linn LIKE ?)"
            params.extend([f"%{location}%", f"%{location}%"])
            
        if status:
            query += " AND status LIKE ?"
            params.append(f"%{status}%")
            
        if person:
            # Person search is harder in SQLite JSON without full relational schema, 
            # but we can use JSON_EACH or simple LIKE on the full_data blob
            query += " AND (full_data LIKE ? OR enrichment LIKE ?)"
            params.extend([f"%{person}%", f"%{person}%"])

        cursor = self.conn.execute(query, params)
        for row in cursor:
            data = json.loads(row['full_data'])
            if row['enrichment']:
                data['enrichment'] = json.loads(row['enrichment'])
            yield data

    def get_stats(self):
        total = self.conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
        enriched = self.conn.execute("SELECT COUNT(*) FROM companies WHERE enrichment IS NOT NULL").fetchone()[0]
        return total, enriched

# ============================================================
# Configuration
# ============================================================

BASE_URL = "https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/"
DATA_FILES = [
    "ettevotja_rekvisiidid__yldandmed.json.zip",
    "ettevotja_rekvisiidid__osanikud.json.zip",
    "ettevotja_rekvisiidid__kasusaajad.json.zip",
    "ettevotja_rekvisiidid__kaardile_kantud_isikud.json.zip",
    "ettevotja_rekvisiidid__registrikaardid.json.zip",
    "ettevotja_rekvisiidid__lihtandmed.csv.zip",
]
CHUNK_SIZE = 50000

# ============================================================
# Utils
# ============================================================

def iter_json_array(filepath: Path):
    """Memory-efficient iteration of JSON array objects."""
    jq_path = shutil.which("jq")
    if jq_path:
        proc = subprocess.Popen(
            ["jq", "-c", ".[]", str(filepath)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for line in proc.stdout:
            if line.strip():
                yield json.loads(line)
        proc.wait()
    else:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        yield from data

# ============================================================
# Download Logic
# ============================================================

class Downloader:
    def __init__(self):
        self.progress = {}
        self.lock = Lock()

    def _print_status(self):
        with self.lock:
            parts = [f"{k.split('__')[-1]}: {v}" for k, v in self.progress.items()]
            line = " | ".join(parts)
            print(f"{line[:120]:<120}", end="", flush=True)

    def _download_one(self, filename, output_path):
        url = BASE_URL + filename
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'ProffyRegistryCLI/1.0'})
            
            # Check for incremental update
            if output_path.exists():
                head_req = urllib.request.Request(url, method='HEAD', headers={'User-Agent': 'ProffyRegistryCLI/1.0'})
                with urllib.request.urlopen(head_req) as resp:
                    remote_size = int(resp.headers.get('content-length', 0))
                    local_size = output_path.stat().st_size
                    if remote_size == local_size:
                        with self.lock: self.progress[filename] = "SKIP (Latest)"
                        self._print_status()
                        return

            with urllib.request.urlopen(req) as resp:
                total = int(resp.headers.get('content-length', 0))
                downloaded = 0
                with open(output_path, 'wb') as f:
                    while True:
                        chunk = resp.read(1024 * 1024)
                        if not chunk: break
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total:
                            with self.lock: self.progress[filename] = f"{(downloaded/total)*100:.0f}%"
                            self._print_status()
            with self.lock: self.progress[filename] = "DONE"
            self._print_status()
        except Exception:
            with self.lock: self.progress[filename] = "ERROR"

    def run(self, base_dir: Path):
        print(f"Downloading to {base_dir}...")
        threads = []
        for f in DATA_FILES:
            path = base_dir / f
            if path.exists():
                self.progress[f] = "SKIP"
                continue
            self.progress[f] = "0%"
            t = Thread(target=self._download_one, args=(f, path))
            threads.append(t)
            t.start()
        for t in threads: t.join()
        print("\nDownload complete.")

# ============================================================
# Merge Logic
# ============================================================

def perform_merge(base_dir: Path, db: RegistryDB = None):
    extracted_dir = base_dir / 'extracted'
    chunks_dir = base_dir / 'chunks'
    extracted_dir.mkdir(exist_ok=True)
    chunks_dir.mkdir(exist_ok=True)

    # ... [Rest of unzipping/merging logic remains same] ...

    # 1. Unzip
    print("Unzipping...")
    extracted = {}
    lock = Lock()
    def unzip_one(zip_name):
        zip_path = base_dir / zip_name
        if not zip_path.exists(): return
        with zipfile.ZipFile(zip_path, 'r') as zf:
            names = zf.namelist()
            if names:
                zf.extractall(extracted_dir)
                with lock: extracted[zip_name] = extracted_dir / names[0]

    threads = [Thread(target=unzip_one, args=(f,)) for f in DATA_FILES]
    for t in threads: t.start()
    for t in threads: t.join()

    # 2. Merge
    print("Merging data (streaming)...")
    merged = {}
    
    # Base CSV
    csv_zip = 'ettevotja_rekvisiidid__lihtandmed.csv.zip'
    if csv_zip in extracted:
        with open(extracted[csv_zip], 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                code = row.get('ariregistri_kood')
                if code:
                    try: code = int(code)
                    except: pass
                    merged[code] = dict(row)
                    merged[code]['ariregistri_kood'] = code

    # yldandmed (General)
    yld_zip = 'ettevotja_rekvisiidid__yldandmed.json.zip'
    if yld_zip in extracted:
        for item in iter_json_array(extracted[yld_zip]):
            code = item.get('ariregistri_kood')
            if not code: continue
            try: code = int(code)
            except: pass
            if code not in merged: merged[code] = {'ariregistri_kood': code, 'nimi': item.get('nimi', '')}
            for k, v in item.items():
                if k not in ('ariregistri_kood', 'nimi'): merged[code][k] = v

    # Nested files
    nested_mapping = {
        'ettevotja_rekvisiidid__osanikud.json.zip': 'osanikud',
        'ettevotja_rekvisiidid__kasusaajad.json.zip': 'kasusaajad',
        'ettevotja_rekvisiidid__kaardile_kantud_isikud.json.zip': 'isikud',
        'ettevotja_rekvisiidid__registrikaardid.json.zip': 'kaardid',
    }
    
    def load_nested(zip_name, key):
        print(f"  Processing {key}...")
        groups = defaultdict(list)
        for item in iter_json_array(extracted[zip_name]):
            code = item.get('ariregistri_kood')
            if code:
                try: code = int(code)
                except: pass
                groups[code].append({k: v for k, v in item.items() if k not in ('ariregistri_kood', 'nimi')})
        with lock:
            for code, items in groups.items():
                if code in merged: merged[code][key] = items

    threads = [Thread(target=load_nested, args=(z, k)) for z, k in nested_mapping.items() if z in extracted]
    for t in threads: t.start()
    for t in threads: t.join()

    # 3. Chunk
    print(f"Writing chunks...")
    sorted_codes = sorted(merged.keys())
    manifest_chunks = []
    for i in range(0, len(sorted_codes), CHUNK_SIZE):
        batch_codes = sorted_codes[i:i+CHUNK_SIZE]
        batch_data = [merged[code] for code in batch_codes]
        
        if db:
            print(f"  Inserting batch {i//CHUNK_SIZE + 1} into database...")
            db.insert_batch(batch_data)

        chunk_name = f"chunk_{(i//CHUNK_SIZE)+1:03d}.json"
        with open(chunks_dir / chunk_name, 'w', encoding='utf-8') as f:
            json.dump(batch_data, f, ensure_ascii=False)
        manifest_chunks.append({
            "file": chunk_name, "count": len(batch_data),
            "start_code": batch_codes[0], "end_code": batch_codes[-1]
        })

    with open(chunks_dir / 'manifest.json', 'w') as f:
        json.dump({"total": len(sorted_codes), "chunks": manifest_chunks}, f, indent=2)

    shutil.rmtree(extracted_dir)
    print(f"Success! {len(sorted_codes)} companies merged into {len(manifest_chunks)} chunks.")

# ============================================================
# Search Logic
# ============================================================

def perform_search(chunks_dir: Path, term: str = None, search_type: str = 'general', 
                   location: str = None, status: str = None, person: str = None,
                   export_path: Path = None, db: RegistryDB = None):
    
    if db:
        results = list(db.search(term=term, person=person, location=location, status=status))
        for item in results:
            if not export_path:
                print("-" * 40)
                print(json.dumps(item, indent=2, ensure_ascii=False))
        found = len(results)
    else:
        manifest_path = chunks_dir / 'manifest.json'
        if not manifest_path.exists():
            print("No data found. Run 'sync' first.")
            return

        with open(manifest_path, 'r') as f:
            manifest = json.load(f)

        results = []
        term_lower = term.lower() if term else None
        loc_lower = location.lower() if location else None
        stat_lower = status.lower() if status else None
        pers_lower = person.lower() if person else None

        for chunk_info in manifest.get('chunks', []):
            chunk_path = chunks_dir / chunk_info['file']
            try:
                with open(chunk_path, 'r', encoding='utf-8') as f:
                    chunk_data = json.load(f)
            except Exception as e:
                print(f"Error reading {chunk_path}: {e}")
                continue

            for item in chunk_data:
                match = True
                
                if term_lower:
                    name_match = term_lower in item.get('nimi', '').lower()
                    code_match = str(item.get('ariregistri_kood')) == term
                    
                    if search_type == 'name' and not name_match: match = False
                    elif search_type == 'code' and not code_match: match = False
                    elif search_type == 'general' and not (name_match or code_match): match = False
                
                if match and loc_lower:
                    addr = (item.get('aadress_maakond', '') + ' ' + item.get('aadress_linn', '')).lower()
                    if loc_lower not in addr: match = False
                
                if match and stat_lower:
                    if stat_lower not in item.get('staatus', '').lower(): match = False

                if match and pers_lower:
                    # Deep search for person
                    raw_str = json.dumps(item).lower()
                    if pers_lower not in raw_str: match = False

                if match:
                    results.append(item)

        for item in results:
            if not export_path:
                print("-" * 40)
                print(json.dumps(item, indent=2, ensure_ascii=False))
        found = len(results)

    if export_path:
        print(f"Exporting {len(results)} results to {export_path}...")
        if export_path.suffix == '.csv':
            if results:
                with open(export_path, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=results[0].keys())
                    writer.writeheader()
                    writer.writerows(results)
        else:
            with open(export_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)

    if not found: print("No results found.")
    else: print(f"Found {found} results.")

# ============================================================
# Enrichment Logic
# ============================================================

ENRICH_URL = "https://ariregister.rik.ee/eng/company/{code}/registry_card_pdf?registry_card_lang=eng"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

def download_registry_pdf(code: str) -> bytes:
    url = ENRICH_URL.format(code=code)
    headers = {'User-Agent': USER_AGENT}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.content

def parse_pdf_content(pdf_bytes: bytes) -> dict:
    with io.BytesIO(pdf_bytes) as f:
        reader = PdfReader(f)
        text = "\n".join(page.extract_text() for page in reader.pages)
    
    id_pattern = re.compile(r'\b([3456]\d{10})\b')
    persons = []
    lines = text.split('\n')
    for line in lines:
        matches = id_pattern.findall(line)
        for id_code in matches:
            clean_line = line.strip()
            name_part = clean_line.replace(id_code, '')
            for keyword in ['personal identification code', 'born', ',', 'Management board member:']:
                name_part = name_part.replace(keyword, '')
            persons.append({
                "personal_id": id_code,
                "name": name_part.strip(),
                "context": clean_line
            })
    return {"persons": persons, "enriched_at": datetime.now().isoformat()}

def perform_enrichment(base_dir: Path, codes: list[str], db: RegistryDB = None):
    print(f"Enriching {len(codes)} companies...")
    
    chunks_dir = base_dir / 'chunks'
    manifest_path = chunks_dir / 'manifest.json'
    manifest = None
    if manifest_path.exists():
        with open(manifest_path, 'r') as f:
            manifest = json.load(f)

    for code_str in codes:
        try:
            code = int(code_str)
        except ValueError:
            print(f"Invalid code: {code_str}")
            continue

        try:
            print(f"Processing {code}...")
            pdf_bytes = download_registry_pdf(str(code))
            enriched_info = parse_pdf_content(pdf_bytes)
            
            if db:
                db.update_enrichment(code, enriched_info)
                print(f"  Updated database.")
            
            # Find in chunks too for consistency
            if manifest:
                target_chunk = None
                for chunk in manifest['chunks']:
                    if chunk['start_code'] <= code <= chunk['end_code']:
                        target_chunk = chunk
                        break
                
                if target_chunk:
                    c_path = chunks_dir / target_chunk['file']
                    if c_path.exists():
                        with open(c_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        
                        updated = False
                        for item in data:
                            if item.get('ariregistri_kood') == code:
                                item['enrichment'] = enriched_info
                                updated = True
                                break
                        
                        if updated:
                            with open(c_path, 'w', encoding='utf-8') as f:
                                json.dump(data, f, ensure_ascii=False)
                            print(f"  Updated {target_chunk['file']}.")
                        else:
                            print(f"  Code not found in {target_chunk['file']}.")
                else:
                    print(f"  No chunk range found for code.")
            
            time.sleep(1)
        except Exception as e:
            print(f"Error enriching {code}: {e}")

def perform_stats(base_dir: Path, db: RegistryDB = None):
    if db:
        total_companies, enriched_count = db.get_stats()
    else:
        chunks_dir = base_dir / 'chunks'
        manifest_path = chunks_dir / 'manifest.json'
        total_companies = 0
        enriched_count = 0
        
        if manifest_path.exists():
            with open(manifest_path, 'r') as f:
                manifest = json.load(f)
                total_companies = manifest.get('total', 0)
            
            print("Scanning chunks for enrichment stats...")
            for chunk in manifest.get('chunks', []):
                try:
                    with open(chunks_dir / chunk['file'], 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        for item in data:
                            if item.get('enrichment'):
                                enriched_count += 1
                except Exception:
                    pass
    
    print("\n" + "=" * 40)
    print("ESTONIAN REGISTRY STATISTICS")
    print("=" * 40)
    print(f"Total Companies: {total_companies:,}")
    print(f"Enriched:        {enriched_count:,} ({ (enriched_count/total_companies*100 if total_companies > 0 else 0):.2f}%)")
    print("=" * 40)

# ============================================================
# Main CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Estonian Registry CLI")
    parser.add_argument("--use-db", action="store_true", help="Use SQLite database backend")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    subparsers.add_parser("download", help="Download data")
    subparsers.add_parser("merge", help="Merge data")
    subparsers.add_parser("sync", help="Download and merge")
    
    p_search = subparsers.add_parser("search", help="Search companies")
    p_search.add_argument("term", nargs="?", help="Search term")
    p_search.add_argument("-n", "--name", action="store_true", help="Search by name")
    p_search.add_argument("-c", "--code", action="store_true", help="Search by code")
    p_search.add_argument("-l", "--location", help="Filter by location")
    p_search.add_argument("-s", "--status", help="Filter by status")
    p_search.add_argument("-p", "--person", help="Filter by person")
    p_search.add_argument("--export", help="Export path (JSON/CSV)")

    p_enrich = subparsers.add_parser("enrich", help="Enrich companies")
    p_enrich.add_argument("codes", nargs="+", help="Registry codes")

    subparsers.add_parser("stats", help="Statistics")
    subparsers.add_parser("ui", help="Launch TUI")

    args = parser.parse_args()
    base_dir = Path(__file__).parent
    db = RegistryDB(base_dir / "registry.db") if args.use_db or (base_dir / "registry.db").exists() else None
    
    if db:
        print("Using SQLite database backend.")

    if args.command == "download":
        Downloader().run(base_dir)
    elif args.command == "merge":
        perform_merge(base_dir, db=db)
    elif args.command == "sync":
        Downloader().run(base_dir)
        perform_merge(base_dir, db=db)
    elif args.command == "search":
        s_type = 'name' if args.name else 'code' if args.code else 'general'
        export_path = Path(args.export) if args.export else None
        perform_search(base_dir / 'chunks', args.term, s_type, 
                       location=args.location, status=args.status, 
                       person=args.person, export_path=export_path, db=db)
    elif args.command == "enrich":
        perform_enrichment(base_dir, args.codes, db=db)
    elif args.command == "stats":
        perform_stats(base_dir, db=db)
    elif args.command == "ui":
        try:
            from tui import RegistryTUI
            app = RegistryTUI(base_dir / 'chunks')
            app.run()
        except ImportError:
            print("Please install textual: uv pip install textual")
        except Exception as e:
            print(f"Error launching TUI: {e}")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()