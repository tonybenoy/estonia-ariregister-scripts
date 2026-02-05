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
import logging

# ============================================================
# Logging Configuration
# ============================================================

logger = logging.getLogger("registry")

def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)

# ============================================================
# Translation Logic
# ============================================================

TRANSLATIONS = {
    "ariregistri_kood": "registry_code",
    "nimi": "name",
    "staatus": "status",
    "aadress_maakond": "county",
    "aadress_linn": "city",
    "aadress_tanav": "street",
    "aadress_maja": "house_number",
    "aadress_korter": "apartment",
    "sihtnumber": "zip_code",
    "ettevotja_aadress": "address",
    "piirkond": "region",
    "ettevotja_oiguslik_vorm": "legal_form",
    "ettevotja_staatus": "company_status",
    "ettevotja_staatus_tekstina": "status_description",
    "esmakande_kuupaev": "incorporation_date",
    "kustutamise_kuupaev": "deletion_date",
    "osanikud": "shareholders",
    "kasusaajad": "beneficiaries",
    "isikud": "persons",
    "kaardid": "registry_cards",
    "enrichment": "enrichment",
    "isikukood_registrikood": "id_code",
    "nimi_arinimi": "name_legal_name",
    "isiku_roll": "role",
    "isiku_aadress": "address",
    "valis_isikukood": "foreign_id_code",
    "valis_riik": "foreign_country",
    "valis_riik_tekstina": "foreign_country_name",
    "osathtede_arv": "shares_count",
    "osamaksu_summa": "contribution_amount",
    "valuuta": "currency",
    "isik_id": "person_id",
    "isiku_liik": "person_type",
    "algus_kuupaev": "start_date",
    "lopp_kuupaev": "end_date",
    "osapoole_liik": "party_type",
    "osaluse_protsent": "ownership_percentage",
    "isiku_nimi": "person_name",
    "kontrolli_liik": "control_type",
}

VALUE_TRANSLATIONS = {
    # Statuses
    "Registrisse kantud": "Entered into register",
    "Kustutatud": "Deleted",
    "Likvideerimisel": "In liquidation",
    "Pankrotis": "Bankrupt",
    "Hoiatuskandega": "With warning entry",
    # Legal Forms
    "Osaühing": "Private limited company",
    "Aktsiaselts": "Public limited company",
    "Füüsilisest isikust ettevõtja": "Sole proprietor",
    "Mittetulundusühing": "Non-profit association",
    "Täisühing": "General partnership",
    "Usaldusühing": "Limited partnership",
    "Korteriühistu": "Apartment association",
    "Sihtasutus": "Foundation",
    # Roles
    "Juhatuse liige": "Management board member",
    "Osanik": "Shareholder",
    "Asutaja": "Founder",
    "Täisosanik": "General partner",
    "Usaldusosanik": "Limited partner",
    "Prokurist": "Proxy holder",
    "Likvideerija": "Liquidator",
    "Pankrotihaldur": "Trustee in bankruptcy",
}

def translate_item(item):
    """Recursively translate keys and common values from Estonian to English, keeping original terms in brackets."""
    if isinstance(item, list):
        return [translate_item(i) for i in item]
    if isinstance(item, dict):
        new_item = {}
        for k, v in item.items():
            translated_key = TRANSLATIONS.get(k, k)
            new_item[translated_key] = translate_item(v)
        return new_item
    if isinstance(item, str):
        translated_value = VALUE_TRANSLATIONS.get(item)
        if translated_value:
            return f"{translated_value} ({item})"
        return item
    return item

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
                    legal_form TEXT,
                    founded_at TEXT,
                    full_data JSON,
                    enrichment JSON
                )
            """)
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_name ON companies(name)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON companies(status)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_founded ON companies(founded_at)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_legal ON companies(legal_form)")

    def insert_batch(self, batch):
        with self.conn:
            self.conn.executemany(
                "INSERT OR REPLACE INTO companies (code, name, status, maakond, linn, legal_form, founded_at, full_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [(
                    item.get('ariregistri_kood'),
                    item.get('nimi'),
                    item.get('staatus'),
                    item.get('aadress_maakond'),
                    item.get('aadress_linn'),
                    item.get('ettevotja_oiguslik_vorm'),
                    item.get('esmakande_kuupaev'),
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

    def get_related_companies(self, code: int):
        """Find companies related by shared persons/shareholders."""
        # 1. Get persons from the target company
        cursor = self.conn.execute("SELECT full_data FROM companies WHERE code = ?", (code,))
        row = cursor.fetchone()
        if not row: return []
        
        data = json.loads(row['full_data'])
        # Extract ID codes of all associated people (shareholders, board members, etc.)
        id_codes = set()
        for key in ['osanikud', 'kasusaajad', 'isikud']:
            for person in data.get(key, []):
                id_code = person.get('isikukood_registrikood')
                if id_code: id_codes.add(id_code)
        
        if not id_codes: return []
        
        # 2. Find other companies containing these ID codes
        related = []
        for id_code in id_codes:
            # We use LIKE here because we're searching inside the JSON blob
            # A more robust way would be a separate 'associations' table, but this works for now
            cursor = self.conn.execute(
                "SELECT full_data FROM companies WHERE code != ? AND full_data LIKE ?", 
                (code, f'%{id_code}%')
            )
            for r in cursor:
                related.append(json.loads(r['full_data']))
        
        return related

    def get_stats(self):
        total = self.conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
        enriched = self.conn.execute("SELECT COUNT(*) FROM companies WHERE enrichment IS NOT NULL").fetchone()[0]
        
        status_counts = self.conn.execute("SELECT status, COUNT(*) as count FROM companies GROUP BY status ORDER BY count DESC LIMIT 5").fetchall()
        county_counts = self.conn.execute("SELECT maakond, COUNT(*) as count FROM companies GROUP BY maakond ORDER BY count DESC LIMIT 5").fetchall()
        
        # New Analytics
        legal_counts = self.conn.execute("SELECT legal_form, COUNT(*) as count FROM companies WHERE legal_form IS NOT NULL GROUP BY legal_form ORDER BY count DESC LIMIT 5").fetchall()
        year_counts = self.conn.execute("SELECT strftime('%Y', founded_at) as year, COUNT(*) as count FROM companies WHERE founded_at IS NOT NULL GROUP BY year ORDER BY year DESC LIMIT 10").fetchall()
        
        return {
            "total": total,
            "enriched": enriched,
            "statuses": dict(status_counts),
            "counties": dict(county_counts),
            "legal_forms": dict(legal_counts),
            "years": dict(year_counts)
        }

# = ===========================================================
# SDK Main Class
# ============================================================

class EstonianRegistry:
    DATA_FILES = [
        "ettevotja_rekvisiidid__yldandmed.json.zip",
        "ettevotja_rekvisiidid__osanikud.json.zip",
        "ettevotja_rekvisiidid__kasusaajad.json.zip",
        "ettevotja_rekvisiidid__kaardile_kantud_isikud.json.zip",
        "ettevotja_rekvisiidid__registrikaardid.json.zip",
        "ettevotja_rekvisiidid__lihtandmed.csv.zip",
    ]

    def __init__(self, data_dir: str = "data", chunk_size: int = 50000):
        self.base_dir = Path.cwd()
        self.data_dir = self.base_dir / data_dir
        self.download_dir = self.data_dir / "downloads"
        self.extracted_dir = self.data_dir / "extracted"
        self.chunks_dir = self.data_dir / "chunks"
        self.db_path = self.data_dir / "registry.db"
        self.chunk_size = chunk_size
        self.base_url = "https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/"
        self.db = RegistryDB(self.db_path) if self.db_path.exists() else None
        self._ensure_dirs()

    def _ensure_dirs(self):
        for d in [self.download_dir, self.extracted_dir, self.chunks_dir]:
            d.mkdir(parents=True, exist_ok=True)

    def download(self):
        """Download all registry data files."""
        return Downloader(self.download_dir, self.base_url, self.DATA_FILES).run()

    def merge(self, use_db: bool = False):
        """Merge downloaded data into chunks and/or database."""
        if use_db and not self.db:
            self.db = RegistryDB(self.db_path)
        
        # 1. Unzip
        logger.info("Unzipping data...")
        extracted = {}
        lock = Lock()
        def unzip_one(zip_name):
            zip_path = self.download_dir / zip_name
            if not zip_path.exists(): return
            try:
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    names = zf.namelist()
                    if names:
                        zf.extractall(self.extracted_dir)
                        with lock: extracted[zip_name] = self.extracted_dir / names[0]
            except Exception as e:
                logger.error(f"Error unzipping {zip_name}: {e}")

        threads = [Thread(target=unzip_one, args=(f,)) for f in self.DATA_FILES]
        for t in threads: t.start()
        for t in threads: t.join()

        # 2. Merge
        logger.info("Merging data (streaming)...")
        merged = {}
        
        # Base CSV
        csv_zip = 'ettevotja_rekvisiidid__lihtandmed.csv.zip'
        if csv_zip in extracted:
            logger.debug("Processing base CSV data...")
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
            logger.debug("Processing general JSON data...")
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
            logger.debug(f"Processing nested data: {key}...")
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
        logger.info(f"Writing chunks...")
        sorted_codes = sorted(merged.keys())
        manifest_chunks = []
        for i in range(0, len(sorted_codes), self.chunk_size):
            batch_codes = sorted_codes[i:i+self.chunk_size]
            batch_data = [merged[code] for code in batch_codes]
            
            if self.db:
                logger.debug(f"Inserting batch {i//self.chunk_size + 1} into database...")
                self.db.insert_batch(batch_data)

            chunk_name = f"chunk_{(i//self.chunk_size)+1:03d}.json"
            with open(self.chunks_dir / chunk_name, 'w', encoding='utf-8') as f:
                json.dump(batch_data, f, ensure_ascii=False)
            manifest_chunks.append({
                "file": chunk_name, "count": len(batch_data),
                "start_code": batch_codes[0], "end_code": batch_codes[-1]
            })

        with open(self.chunks_dir / 'manifest.json', 'w') as f:
            json.dump({"total": len(sorted_codes), "chunks": manifest_chunks}, f, indent=2)

        shutil.rmtree(self.extracted_dir)
        logger.info(f"Success! {len(sorted_codes)} companies merged into {len(manifest_chunks)} chunks.")

    def sync(self, use_db: bool = False, force: bool = False):
        """Download and merge data in one step."""
        changed = self.download()
        if changed or force:
            self.merge(use_db=use_db)
        else:
            logger.info("No new data downloaded. Skipping merge (use --force to override).")

    def search(self, term: str = None, search_type: str = 'general', 
               location: str = None, status: str = None, person: str = None,
               translate: bool = False, limit: int = None):
        """Search companies and return a list of results."""
        results = []
        if self.db:
            results = list(self.db.search(term=term, person=person, location=location, status=status))
            if translate:
                results = [translate_item(r) for r in results]
        else:
            manifest_path = self.chunks_dir / 'manifest.json'
            if not manifest_path.exists():
                raise FileNotFoundError("No data found. Run sync() first.")

            with open(manifest_path, 'r') as f:
                manifest = json.load(f)

            term_lower = term.lower() if term else None
            loc_lower = location.lower() if location else None
            stat_lower = status.lower() if status else None
            pers_lower = person.lower() if person else None

            for chunk_info in manifest.get('chunks', []):
                chunk_path = self.chunks_dir / chunk_info['file']
                try:
                    with open(chunk_path, 'r', encoding='utf-8') as f:
                        chunk_data = json.load(f)
                except Exception:
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
                        raw_str = json.dumps(item).lower()
                        if pers_lower not in raw_str: match = False

                    if match:
                        res = translate_item(item) if translate else item
                        results.append(res)
                    
                    if limit and len(results) >= limit: break
                if limit and len(results) >= limit: break
        
        return results

    def enrich(self, codes: list[str]):
        """Enrich specific registry codes with PDF data."""
        manifest_path = self.chunks_dir / 'manifest.json'
        manifest = None
        if manifest_path.exists():
            with open(manifest_path, 'r') as f:
                manifest = json.load(f)

        for code_str in codes:
            try:
                code = int(code_str)
                logger.info(f"Enriching company {code}...")
                pdf_bytes = download_registry_pdf(str(code))
                enriched_info = parse_pdf_content(pdf_bytes)
                
                if self.db:
                    self.db.update_enrichment(code, enriched_info)
                    logger.debug(f"Updated database for {code}")
                
                if manifest:
                    target_chunk = None
                    for chunk in manifest['chunks']:
                        if chunk['start_code'] <= code <= chunk['end_code']:
                            target_chunk = chunk
                            break
                    if target_chunk:
                        c_path = self.chunks_dir / target_chunk['file']
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
                                logger.debug(f"Updated chunk {target_chunk['file']} for {code}")
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error enriching {code_str}: {e}")

    def find_related(self, code: int, translate: bool = False):
        """Find companies related to the given registry code."""
        if not self.db:
            raise RuntimeError("Network analysis requires SQLite database. Run merge --use-db first.")
        
        results = self.db.get_related_companies(code)
        if translate:
            results = [translate_item(r) for r in results]
        return results

    def get_analytics(self):
        """Return detailed statistics and analytics."""
        stats = {
            "total": 0, "enriched": 0, "statuses": defaultdict(int),
            "counties": defaultdict(int), "legal_forms": defaultdict(int),
            "years": defaultdict(int), "storage": {}
        }

        if self.db:
            db_stats = self.db.get_stats()
            stats.update(db_stats)
        else:
            manifest_path = self.chunks_dir / 'manifest.json'
            if manifest_path.exists():
                with open(manifest_path, 'r') as f:
                    manifest = json.load(f)
                    stats["total"] = manifest.get('total', 0)
                for chunk in manifest.get('chunks', []):
                    try:
                        with open(self.chunks_dir / chunk['file'], 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            for item in data:
                                if item.get('enrichment'): stats["enriched"] += 1
                                stats["statuses"][item.get('staatus', 'Unknown')] += 1
                                stats["counties"][item.get('aadress_maakond', 'Unknown')] += 1
                                stats["legal_forms"][item.get('ettevotja_oiguslik_vorm', 'Unknown')] += 1
                                founded = item.get('esmakande_kuupaev')
                                if founded: stats["years"][founded.split('-')[0]] += 1
                    except Exception: continue
        
        stats["storage"] = {
            "database_mb": (self.db_path.stat().st_size / (1024*1024)) if self.db_path.exists() else 0,
            "chunks_mb": get_dir_size(self.chunks_dir) / (1024*1024),
            "downloads_mb": get_dir_size(self.download_dir) / (1024*1024)
        }
        return stats

# ============================================================
# Configuration Management
# ============================================================


DEFAULT_CONFIG = {
    "data_dir": "data",
    "chunk_size": 50000,
    "base_url": "https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/"
}

class Config:
    def __init__(self, config_path: Path):
        self.path = config_path
        self.values = DEFAULT_CONFIG.copy()
        self.load()

    def load(self):
        if self.path.exists():
            with open(self.path, "r") as f:
                self.values.update(json.load(f))

    def save(self):
        with open(self.path, "w") as f:
            json.dump(self.values, f, indent=4)

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value):
        # Type conversion for common keys
        if key == "chunk_size":
            value = int(value)
        self.values[key] = value
        self.save()

# Path Configuration
BASE_DIR = Path(__file__).parent
config = Config(BASE_DIR / "config.json")

# ============================================================
# Utils
# ============================================================

def get_dir_size(path: Path):
    """Calculate total size of a directory in bytes."""
    return sum(f.stat().st_size for f in path.glob('**/*') if f.is_file())

def download_registry_pdf(code: str):
    """Download a company's registry PDF from the official portal."""
    url = f"https://ariregister.rik.ee/eng/company/{code}/registry_card_pdf?registry_card_lang=eng"
    browser_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    
    try:
        req = urllib.request.Request(url, headers={'User-Agent': browser_agent})
        with urllib.request.urlopen(req) as resp:
            return resp.read()
    except Exception as e:
        logger.error(f"Failed to download PDF for {code}: {e}")
        return b""

def parse_pdf_content(pdf_bytes: bytes):
    """Extract information from registry PDF bytes."""
    if not pdf_bytes:
        return {}
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        full_text = ""
        for page in reader.pages:
            full_text += page.extract_text() + "\n"
        
        info = {
            "processed_at": datetime.now().isoformat(),
            "pages": len(reader.pages),
            "raw_text_snippet": full_text[:500].replace("\n", " "),
        }
        
        # Simple extraction for some common fields if they exist in the PDF
        # Note: Registry cards vary in format, but we can try some regex
        caps_match = re.search(r"Capital:\s*([\d\s,]+)\s*([A-Z]{3})", full_text)
        if caps_match:
            info["capital"] = caps_match.group(1).strip()
            info["currency"] = caps_match.group(2)
            
        return info
    except Exception as e:
        logger.error(f"Error parsing PDF: {e}")
        return {}

def format_company_card(item):
    """Format company information into a readable card."""
    # Determine which keys to use (handle both Estonian and English if translated)
    name = item.get('name') or item.get('nimi') or 'Unknown'
    code = item.get('registry_code') or item.get('ariregistri_kood') or 'Unknown'
    status = item.get('status_description') or item.get('ettevotja_staatus_tekstina') or 'Unknown'
    address = item.get('address') or item.get('ads_normaliseeritud_taisaadress') or item.get('ettevotja_aadress') or 'No address'
    legal_form = item.get('legal_form') or item.get('ettevotja_oiguslik_vorm') or 'Unknown'
    
    # Capital info
    capital_str = "Unknown"
    yld = item.get('yldandmed', {})
    if isinstance(yld, dict):
        caps = yld.get('kapitalid', [])
        if caps and isinstance(caps, list):
            cap = caps[0]
            val = cap.get('kapitali_suurus')
            curr = cap.get('kapitali_valuuta')
            if val and curr:
                capital_str = f"{val} {curr}"

    # Board Members
    persons = []
    isikud_list = item.get('isikud', [])
    if isikud_list and isinstance(isikud_list, list):
        for entry in isikud_list:
            sub_list = entry.get('kaardile_kantud_isikud', [])
            for p in sub_list:
                p_name = f"{p.get('eesnimi', '')} {p.get('nimi_arinimi', '')}".strip()
                role = p.get('isiku_roll_tekstina', 'Member')
                if p_name:
                    persons.append(f"{p_name} ({role})")

    card = [
        f"\033[1;34m{name}\033[0m",
        f"  \033[1mRegistry Code:\033[0m {code}",
        f"  \033[1mLegal Form:\033[0m    {legal_form}",
        f"  \033[1mStatus:\033[0m        {status}",
        f"  \033[1mCapital:\033[0m       {capital_str}",
        f"  \033[1mAddress:\033[0m       {address}",
    ]
    
    if persons:
        card.append(f"  \033[1mBoard:\033[0m         {', '.join(persons[:3])}{'...' if len(persons) > 3 else ''}")
    
    if item.get('enrichment'):
        e = item['enrichment']
        card.append(f"  \033[1mEnrichment:\033[0m    PDF ({e.get('pages', '?')} pgs) processed on {e.get('processed_at', '')[:10]}")

    return "\n".join(card)

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
    def __init__(self, download_dir: Path, base_url: str, data_files: list[str]):
        self.progress = {}
        self.lock = Lock()
        self.download_dir = download_dir
        self.base_url = base_url
        self.data_files = data_files
        self.new_downloads = 0

    def _print_status(self):
        with self.lock:
            parts = [f"{k.split('__')[-1]}: {v}" for k, v in self.progress.items()]
            line = " | ".join(parts)
            print(f"{line[:120]:<120}", end="", flush=True)

    def _download_one(self, filename, output_path):
        url = self.base_url + filename
        browser_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        try:
            req = urllib.request.Request(url, headers={'User-Agent': browser_agent})
            
            # Check for incremental update
            if output_path.exists():
                head_req = urllib.request.Request(url, method='HEAD', headers={'User-Agent': browser_agent})
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
            with self.lock: 
                self.progress[filename] = "DONE"
                self.new_downloads += 1
            self._print_status()
        except Exception:
            with self.lock: self.progress[filename] = "ERROR"

    def run(self):
        logger.info(f"Downloading data to {self.download_dir}...")
        threads = []
        for f in self.data_files:
            path = self.download_dir / f
            self.progress[f] = "0%"
            t = Thread(target=self._download_one, args=(f, path))
            threads.append(t)
            t.start()
        for t in threads: t.join()
        logger.info(f"Download process complete. {self.new_downloads} new files.")
        return self.new_downloads > 0

# ============================================================
# Main CLI
# ============================================================


def main():
    # Common arguments for all commands, allowing them to be used before or after subcommands
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument("--use-db", action="store_true", help="Use SQLite database backend")
    parent_parser.add_argument("--data-dir", default="data", help="Data directory")
    parent_parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")

    parser = argparse.ArgumentParser(description="Estonian Registry CLI", parents=[parent_parser])
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    subparsers.add_parser("download", help="Download data", parents=[parent_parser])
    subparsers.add_parser("merge", help="Merge data", parents=[parent_parser])
    subparsers.add_parser("sync", help="Download and merge", parents=[parent_parser])
    
    p_search = subparsers.add_parser("search", help="Search companies", parents=[parent_parser])
    p_search.add_argument("term", nargs="?", help="Search term")
    p_search.add_argument("-n", "--name", action="store_true", help="Search by name")
    p_search.add_argument("-c", "--code", action="store_true", help="Search by code")
    p_search.add_argument("-l", "--location", help="Filter by location")
    p_search.add_argument("-s", "--status", help="Filter by status")
    p_search.add_argument("-p", "--person", help="Filter by person")
    p_search.add_argument("--export", help="Export path (JSON/CSV)")
    p_search.add_argument("-t", "--translate", action="store_true", help="Translate keys and values to English")
    p_search.add_argument("--limit", type=int, help="Limit number of results")

    p_enrich = subparsers.add_parser("enrich", help="Enrich companies", parents=[parent_parser])
    p_enrich.add_argument("codes", nargs="+", help="Registry codes")

    subparsers.add_parser("stats", help="Statistics", parents=[parent_parser])

    p_network = subparsers.add_parser("network", help="Find related companies (Network Analysis)", parents=[parent_parser])
    p_network.add_argument("code", type=int, help="Registry code to analyze")
    p_network.add_argument("-t", "--translate", action="store_true", help="Translate to English")

    p_config = subparsers.add_parser("config", help="Manage configuration", parents=[parent_parser])
    p_config.add_argument("action", choices=["show", "set"], help="Action to perform")
    p_config.add_argument("key", nargs="?", help="Config key")
    p_config.add_argument("value", nargs="?", help="Config value")

    args = parser.parse_args()
    setup_logging(args.verbose)
    
    if args.command == "config":
        if args.action == "show":
            print(json.dumps(config.values, indent=4))
        elif args.action == "set":
            if not args.key or not args.value:
                logger.error("Usage: registry.py config set <key> <value>")
            else:
                config.set(args.key, args.value)
                logger.info(f"Set {args.key} to {args.value}")
        return

    # Initialize SDK
    reg = EstonianRegistry(data_dir=args.data_dir, chunk_size=config.get("chunk_size"))
    
    if reg.db:
        logger.info(f"Using SQLite database at {reg.db_path}")

    if args.command == "download":
        reg.download()
    elif args.command == "merge":
        reg.merge(use_db=args.use_db)
    elif args.command == "sync":
        reg.sync(use_db=args.use_db)
    elif args.command == "search":
        s_type = 'name' if args.name else 'code' if args.code else 'general'
        results = reg.search(args.term, s_type, 
                           location=args.location, status=args.status, 
                           person=args.person, translate=args.translate,
                           limit=args.limit)
        
        if args.export:
            export_path = Path(args.export)
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
        else:
            for item in results:
                print("-" * 60)
                print(format_company_card(item))
            print(f"\nFound {len(results)} results.")

    elif args.command == "enrich":
        reg.enrich(args.codes)
    elif args.command == "network":
        try:
            results = reg.find_related(args.code, translate=args.translate)
            if not results:
                print("No related companies found.")
            else:
                print(f"Found {len(results)} related companies:")
                for item in results:
                    print("-" * 40)
                    print(json.dumps(item, indent=2, ensure_ascii=False))
        except Exception as e:
            logger.error(e)
    elif args.command == "stats":
        stats = reg.get_analytics()
        # Reusing the pretty print logic from analytics
        print("\n" + "=" * 50)
        print("ESTONIAN REGISTRY ANALYTICS".center(50))
        print("=" * 50)
        print(f"{'Total Companies:':<25} {stats['total']:,}")
        print(f"{'Enriched:':<25} {stats['enriched']:,} ({ (stats['enriched']/stats['total']*100 if stats['total'] > 0 else 0):.2f}%)")
        
        print("\n" + "-" * 50)
        print("LEGAL FORM DISTRIBUTION".center(50))
        top_legal = sorted(stats["legal_forms"].items(), key=lambda x: x[1], reverse=True)[:5]
        for form, count in top_legal:
            print(f"  {form:<35} {count:,}")

        print("\n" + "-" * 50)
        print("TOP COUNTIES".center(50))
        top_counties = sorted(stats["counties"].items(), key=lambda x: x[1], reverse=True)[:5]
        for county, count in top_counties:
            print(f"  {county:<35} {count:,}")

        print("\n" + "-" * 50)
        print("FOUNDING TREND (LAST 10 YEARS)".center(50))
        recent_years = sorted(stats["years"].items(), key=lambda x: x[0], reverse=True)[:10]
        for year, count in recent_years:
            print(f"  {year:<35} {count:,}")

        print("\n" + "-" * 50)
        print("STORAGE USAGE".center(50))
        print(f"  {'SQLite Database:':<25} {stats['storage']['database_mb']:.1f} MB")
        print(f"  {'JSON Chunks:':<25} {stats['storage']['chunks_mb']:.1f} MB")
        print(f"  {'Raw Downloads:':<25} {stats['storage']['downloads_mb']:.1f} MB")
        print("=" * 50 + "\n")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()