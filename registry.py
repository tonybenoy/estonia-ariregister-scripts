#!/usr/bin/env python3
"""
Estonian Registry CLI - High Performance, Beautiful & Multi-Backend
"""

import argparse
import csv
import json
import os
import shutil
import subprocess
import time
import urllib.request
import zipfile
import io
import re
import sqlite3
import sys
from pathlib import Path
from threading import Thread, Lock
from pypdf import PdfReader
from collections import defaultdict
from datetime import datetime
import logging
from abc import ABC, abstractmethod

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.tree import Tree
from rich.columns import Columns
from rich.theme import Theme
from rich.syntax import Syntax
from rich import box

# ============================================================
# Logging & Translation
# ============================================================

logger = logging.getLogger("registry")
console = Console(theme=Theme({
    "info": "dim cyan",
    "warning": "magenta",
    "danger": "bold red",
    "success": "bold green",
    "header": "bold blue",
    "label": "bold yellow"
}))

def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)

TRANSLATIONS = {
    "ariregistri_kood": "registry_code", "nimi": "name", "staatus": "status",
    "aadress_maakond": "county", "aadress_linn": "city", "ettevotja_aadress": "address",
    "asukoht_ettevotja_aadressis": "location_in_address", "asukoha_ehak_kood": "location_ehak_code",
    "asukoha_ehak_tekstina": "location_description", "indeks_ettevotja_aadressis": "postal_code",
    "ettevotja_oiguslik_vorm": "legal_form", "ettevotja_oigusliku_vormi_alaliik": "legal_form_subtype",
    "ettevotja_staatus": "company_status", "ettevotja_staatus_tekstina": "status_description",
    "esmakande_kuupaev": "incorporation_date", "ettevotja_esmakande_kpv": "incorporation_date",
    "kustutamise_kuupaev": "deletion_date", "osanikud": "shareholders", "kasusaajad": "beneficiaries",
    "isikud": "persons", "kaardid": "registry_cards", "isikukood_registrikood": "id_code",
    "isikukood_hash": "id_hash", "nimi_arinimi": "name_legal_name", "isiku_roll": "role",
    "isiku_roll_tekstina": "role_description", "isiku_aadress": "person_address",
    "osathtede_arv": "shares_count", "osamaksu_summa": "contribution_amount", "valuuta": "currency",
    "osaluse_protsent": "ownership_percentage", "kontrolli_liik": "control_type", "yldandmed": "general_data",
    "kapitalid": "capital", "kapitali_suurus": "capital_amount", "kapitali_valuuta": "capital_currency",
    "sidevahendid": "contacts", "liik_tekstina": "type_description", "sisu": "content",
    "teatatud_tegevusalad": "activities", "emtak_kood": "emtak_code", "emtak_tekstina": "activity_description",
    "on_pohitegevusala": "is_main_activity", "info_majandusaasta_aruannetest": "annual_reports",
    "majandusaasta_perioodi_lopp_kpv": "period_end_date", "tootajate_arv": "employee_count",
    "registrikaardid": "registry_entries", "kanded": "entries", "kandeliik_tekstina": "entry_type",
    "kpv": "date", "kande_nr": "entry_number", "kaardi_piirkond": "card_region", "kaardi_nr": "card_number",
    "kaardi_tyyp": "card_type", "eesnimi": "first_name", "isiku_nimi": "person_name",
    "kontrolli_teostamise_viis_tekstina": "control_method", "aadress_riik_tekstina": "country",
    "ads_normaliseeritud_taisaadress": "normalized_address", "teabesysteemi_link": "portal_link",
    "algus_kpv": "start_date", "lopp_kpv": "end_date", "esindusoiguse_normaalregulatsioonid": "standard_representation_rights",
    "esindusoiguse_eritingimused": "special_representation_rights", "markused_kaardil": "registry_annotations",
    "oigusjargsused": "succession_mergers", "staatused": "status_history", "arinimed": "name_history",
    "aadressid": "address_history", "oiguslikud_vormid": "legal_form_history", "kapitalid": "capital_history",
    "enrichment": "enrichment", "processed_at": "processed_at", "unmasked_ids": "unmasked_ids",
    "tegutseb_tekstina": "is_active", "on_raamatupidamiskohustuslane": "is_accounting_obligated",
}

VALUE_TRANSLATIONS = {
    "Registrisse kantud": "Entered into register", "Kustutatud": "Deleted", "Likvideerimisel": "In liquidation",
    "Pankrotis": "Bankrupt", "Hoiatuskandega": "With warning entry", "Registrist kustutatud": "Deleted from register",
    "Osaühing": "Private limited company", "Aktsiaselts": "Public limited company",
    "Füüsilisest isikust ettevõtja": "Sole proprietor", "Mittetulundusühing": "Non-profit association",
    "Täisühing": "General partnership", "Usaldusühing": "Limited partnership",
    "Korteriühistu": "Apartment association", "Sihtasutus": "Foundation", "OÜ": "PLC", "AS": "JSC",
    "Juhatuse liige": "Management board member", "Osanik": "Shareholder", "Asutaja": "Founder",
    "Prokurist": "Proxy holder", "Likvideerija": "Liquidator", "Pankrotihaldur": "Trustee in bankruptcy",
    "Täisosanik": "General partner", "Usaldusosanik": "Limited partner", "otsene osalus": "direct ownership",
    "kaudne osalus": "indirect ownership", "muu kontrolli viis": "other form of control",
    "Esmakanne": "First entry", "Muutmiskanne": "Change entry", "Lõpetamiskanne": "Termination entry",
    "Tegevuse jätkamise kanne": "Continuation of activities entry", "Märkus": "Note",
    "Kapitali muutmise kanne": "Capital change entry", "Ärinime muutmise kanne": "Name change entry",
    "Asutamiskanne": "Founding entry", "Elektronposti aadress": "Email address", "Mobiiltelefon": "Mobile phone",
    "Telefon": "Telephone", "Interneti WWW aadress": "Website", "Faks": "Fax",
    "Programmeerimine": "Programming", "Mootorsõidukite jaemüük": "Retail sale of motor vehicles",
    "Jah": "Yes", "Ei": "No",
    "Osaühingut võib kõikide tehingute tegemisel esindada iga juhatuse liige.": "The private limited company may be represented by any member of the management board in all transactions.",
}

UI_LABELS = {
    "et": {
        "dossier": "Toimik", "core": "Põhiandmed", "enrichment": "PDF-i lisandmed", "general": "Üldatribuudid",
        "history": "Ajaloolised kirjed", "personnel": "Personal ja esindusõigus", "ownership": "Omandisuhted ja pandid",
        "beneficiaries": "Tegelikud kasusaajad", "operations": "Tegevus ja aruandlus", "registry": "Registrikaardi kanded",
        "attr": "Atribuut", "val": "Väärtus", "unmasked_personal_ids": "Avalikustatud isikukoodid", "codes_unmasked": "koodi unmasked",
        "status_hist": "Staatused", "name_hist": "Ärinimed", "addr_hist": "Aadressid", "legal_hist": "Õiguslikud vormid",
        "cap_hist": "Kapitalid", "mergers": "Õigusjärgsus/Ühinemised", "annotations": "Märkused kaardil", "name": "Nimi",
        "id_code": "Isikukood / Reg. kood", "role": "Roll", "since": "Alates", "details": "Detailid", "rights": "Esindusõiguse regulatsioonid",
        "owner": "Omanik", "amount": "Summa", "type": "Tüüp", "control": "Kontrolli viis", "address": "Aadress",
        "activities": "Tegevusalad", "reports": "Majandusaasta aruanded", "contacts": "Sidevahendid", "period_end": "Perioodi lõpp",
        "employees": "Töötajad", "activity": "Tegevusala", "main": "Põhitegevus", "date": "Kuupäev", "entry_type": "Kande liik",
        "entry_num": "Nr", "portal_link": "Link registrisse", "privacy_note": "* Eraisikute isikukoodid on avaandmetes peidetud (hash). PDF-i rikastamine unmaskib need.",
        "results_found": "Leitud tulemusi", "no_results": "Tulemusi ei leitud.",
    },
    "en": {
        "dossier": "Dossier", "core": "Core Identity", "enrichment": "Live PDF Enrichment", "general": "General Attributes",
        "history": "Historical Records", "personnel": "Personnel & Representation", "ownership": "Ownership & Pledges",
        "beneficiaries": "Ultimate Beneficiaries", "operations": "Operations & Reporting", "registry": "Registry Log",
        "attr": "Attribute", "val": "Value", "unmasked_personal_ids": "Unmasked Personal IDs", "codes_unmasked": "codes unmasked",
        "status_hist": "Status History", "name_hist": "Name History", "addr_hist": "Address History", "legal_hist": "Legal Form History",
        "cap_hist": "Capital History", "mergers": "Succession/Mergers", "annotations": "Registry Annotations", "name": "Name",
        "id_code": "ID Code", "role": "Role", "since": "Since", "details": "Details", "rights": "Representation Rights",
        "owner": "Owner", "amount": "Amount", "type": "Type", "control": "Control Type", "address": "Address",
        "activities": "Activities", "reports": "Annual Reports", "contacts": "Contacts", "period_end": "Period End",
        "employees": "Employees", "activity": "Activity", "main": "Main", "date": "Date", "entry_type": "Entry Type",
        "entry_num": "Number", "portal_link": "Portal Link", "privacy_note": "* Personal ID codes for individuals are hashed in open data. PDF enrichment unmasks them.",
        "results_found": "Found results", "no_results": "No results found.",
    }
}

def translate_value(val, to_en=False):
    if not to_en or not isinstance(val, str): return val
    if val in VALUE_TRANSLATIONS: return f"{VALUE_TRANSLATIONS[val]} ({val})"
    if ", " in val:
        parts = [VALUE_TRANSLATIONS.get(p.strip(), p.strip()) for p in val.split(",")]
        if any(p in VALUE_TRANSLATIONS for p in [x.strip() for x in val.split(",")]): return f"{', '.join(parts)} ({val})"
    return val

def translate_item(item, to_en=False):
    if not to_en: return item
    if isinstance(item, list): return [translate_item(i, True) for i in item]
    if isinstance(item, dict): return {TRANSLATIONS.get(k, k): translate_item(v, True) for k, v in item.items()}
    if isinstance(item, str): return translate_value(item, True)
    return item

# ============================================================
# Database Interfaces & Backends
# ============================================================

class RegistryBackend(ABC):
    """Abstract Base Class for all database backends."""
    @abstractmethod
    def insert_batch_base(self, batch): pass
    @abstractmethod
    def update_batch_json(self, key, data_map): pass
    @abstractmethod
    def update_batch_general(self, batch): pass
    @abstractmethod
    def update_enrichment(self, code: int, enrichment: dict): pass
    @abstractmethod
    def search(self, term=None, person=None, location=None, status=None, limit=None): pass
    @abstractmethod
    def is_file_processed(self, filename: str): pass
    @abstractmethod
    def mark_file_status(self, filename: str, status: str): pass
    @abstractmethod
    def get_stats(self): pass
    @abstractmethod
    def commit(self): pass

class SQLiteBackend(RegistryBackend):
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self):
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    code INTEGER PRIMARY KEY, name TEXT, status TEXT, maakond TEXT, linn TEXT, 
                    legal_form TEXT, founded_at TEXT, full_data JSON DEFAULT '{}', enrichment JSON
                )
            """)
            self.conn.execute("CREATE TABLE IF NOT EXISTS sync_state (filename TEXT PRIMARY KEY, status TEXT)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_name ON companies(name)")

    def insert_batch_base(self, batch):
        with self.conn:
            self.conn.executemany(
                "INSERT OR REPLACE INTO companies (code, name, status, maakond, linn, legal_form, founded_at, full_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [(i.get('ariregistri_kood'), i.get('nimi'), i.get('staatus'), i.get('aadress_maakond'), 
                  i.get('aadress_linn'), i.get('ettevotja_oiguslik_vorm'), i.get('esmakande_kuupaev'), json.dumps(i)) for i in batch]
            )

    def update_batch_json(self, key, data_map):
        with self.conn:
            self.conn.executemany(
                "UPDATE companies SET full_data = json_set(full_data, ?, json(?)) WHERE code = ?",
                [(f"$.{key}", json.dumps(val), code) for code, val in data_map.items()]
            )

    def update_batch_general(self, batch):
        with self.conn:
            for item in batch:
                code = item.get('ariregistri_kood')
                if code: self.conn.execute("UPDATE companies SET full_data = json_patch(full_data, ?) WHERE code = ?", (json.dumps(item), code))

    def update_enrichment(self, code: int, enrichment: dict):
        with self.conn: self.conn.execute("UPDATE companies SET enrichment = ? WHERE code = ?", (json.dumps(enrichment), code))

    def search(self, term=None, person=None, location=None, status=None, limit=None):
        query = "SELECT * FROM companies WHERE 1=1"; params = []
        if term:
            if term.isdigit(): query += " AND code = ?"; params.append(int(term))
            else: query += " AND name LIKE ?"; params.append(f"%{term}%")
        if location: query += " AND (maakond LIKE ? OR linn LIKE ?)"; params.extend([f"%{location}%", f"%{location}%"])
        if status: query += " AND (status LIKE ? OR full_data LIKE ?)"; params.extend([f"%{status}%", f"%{status}%"])
        if person: query += " AND (full_data LIKE ? OR enrichment LIKE ?)"; params.extend([f"%{person}%", f"%{person}%"])
        if limit: query += f" LIMIT {int(limit)}"
        for row in self.conn.execute(query, params):
            data = json.loads(row['full_data'])
            if row['enrichment']: data['enrichment'] = json.loads(row['enrichment'])
            yield data

    def is_file_processed(self, filename: str):
        return self.conn.execute("SELECT 1 FROM sync_state WHERE filename=? AND status='DONE'", (filename,)).fetchone() is not None

    def mark_file_status(self, filename: str, status: str):
        with self.conn: self.conn.execute("INSERT OR REPLACE INTO sync_state VALUES (?, ?)", (filename, status))

    def get_stats(self):
        total = self.conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
        enriched = self.conn.execute("SELECT COUNT(*) FROM companies WHERE enrichment IS NOT NULL").fetchone()[0]
        return {"total": total, "enriched": enriched}

    def commit(self): self.conn.commit()

# ============================================================
# Registry Logic
# ============================================================

class EstonianRegistry:
    DATA_FILES = [
        "ettevotja_rekvisiidid__lihtandmed.csv.zip", "ettevotja_rekvisiidid__yldandmed.json.zip",
        "ettevotja_rekvisiidid__osanikud.json.zip", "ettevotja_rekvisiidid__kasusaajad.json.zip",
        "ettevotja_rekvisiidid__kaardile_kantud_isikud.json.zip", "ettevotja_rekvisiidid__registrikaardid.json.zip",
    ]

    def __init__(self, data_dir="data", chunk_size=50000, backend: RegistryBackend = None):
        self.data_dir = Path(data_dir); self.download_dir = self.data_dir / "downloads"
        self.extracted_dir = self.data_dir / "extracted"; self.db_path = self.data_dir / "registry.db"
        for d in [self.download_dir, self.extracted_dir]: d.mkdir(parents=True, exist_ok=True)
        self.db = backend or SQLiteBackend(self.db_path); self.chunk_size = chunk_size

    def sync(self, force=False):
        Downloader(self.download_dir, self.DATA_FILES).run()
        self.merge(force=force)

    def merge(self, force=False):
        logger.info("Starting Merge...")
        extracted = {}
        for f in self.DATA_FILES:
            zp = self.download_dir / f
            if zp.exists():
                with zipfile.ZipFile(zp, 'r') as zf:
                    zf.extractall(self.extracted_dir); extracted[f] = self.extracted_dir / zf.namelist()[0]
        for f, path in extracted.items():
            if not force and self.db.is_file_processed(f):
                logger.info(f"Skipping {f}"); continue
            logger.info(f"Processing {f}...")
            if f.endswith('.csv.zip'):
                batch = []
                with open(path, 'r', encoding='utf-8-sig') as csvf:
                    for row in csv.DictReader(csvf, delimiter=';'):
                        if row.get('ariregistri_kood'):
                            row['ariregistri_kood'] = int(row['ariregistri_kood']); batch.append(row)
                        if len(batch) >= self.chunk_size: self.db.insert_batch_base(batch); batch = []
                if batch: self.db.insert_batch_base(batch)
            elif 'yldandmed' in f:
                batch = []
                for item in iter_json_array(path):
                    if item.get('ariregistri_kood'):
                        item['ariregistri_kood'] = int(item['ariregistri_kood']); batch.append(item)
                    if len(batch) >= self.chunk_size: self.db.update_batch_general(batch); batch = []
                if batch: self.db.update_batch_general(batch)
            else:
                key = f.split('__')[-1].split('.')[0]; groups = defaultdict(list); count = 0
                for item in iter_json_array(path):
                    code = item.get('ariregistri_kood')
                    if code: groups[int(code)].append(item.get(key, item)); count += 1
                    if count >= self.chunk_size: self.db.update_batch_json(key, groups); groups = defaultdict(list); count = 0
                if groups: self.db.update_batch_json(key, groups)
            self.db.mark_file_status(f, 'DONE'); self.db.commit()

    def enrich(self, codes: list[str]):
        for code_str in codes:
            try:
                code = int(code_str); console.print(f"[info]Enriching {code}...[/info]")
                pdf_bytes = download_registry_pdf(str(code))
                if not pdf_bytes: continue
                self.db.update_enrichment(code, parse_pdf_content(pdf_bytes))
                console.print(f"[success]Updated {code}[/success]"); time.sleep(1)
            except Exception as e: logger.error(f"Error enriching {code_str}: {e}")

    def export(self, output_path: Path, translate: bool = False):
        logger.info(f"Exporting to {output_path}...")
        results = []
        for row in self.db.search(limit=None):
            if translate: row = translate_item(row, to_en=True)
            results.append(row)
        with open(output_path, 'w', encoding='utf-8') as f: json.dump(results, f, ensure_ascii=False, indent=2)

# ============================================================
# Utilities & PDF
# ============================================================

def download_registry_pdf(code: str):
    url = f"https://ariregister.rik.ee/eng/company/{code}/registry_card_pdf?registry_card_lang=eng"
    agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': agent})
        with urllib.request.urlopen(req) as resp: return resp.read()
    except Exception as e: logger.error(f"Failed PDF download: {e}"); return None

def parse_pdf_content(pdf_bytes: bytes):
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        full_text = "".join([page.extract_text() + "\n" for page in reader.pages])
        info = {"processed_at": datetime.now().isoformat(), "pages": len(reader.pages), "unmasked_ids": {}}
        caps_match = re.search(r"Capital:\s*([\d\s,]+)\s*([A-Z]{3})", full_text)
        if caps_match:
            info["capital"] = caps_match.group(1).strip(); info["currency"] = caps_match.group(2)
        id_regex = re.compile(r"([1-6]\d{10})"); lines = [l.strip() for l in full_text.split("\n") if l.strip()]
        for i, line in enumerate(lines):
            id_matches = id_regex.findall(line)
            if id_matches:
                for p_id in id_matches:
                    name_regex = re.compile(r"([A-ZŠŽÕÄÖÜ][A-ZŠŽÕÄÖÜa-zšžõäöü\-]+\s+[A-ZŠŽÕÄÖÜ][A-ZŠŽÕÄÖÜa-zšžõäöü\-]+(?:\s+[A-ZŠŽÕÄÖÜ][A-ZŠŽÕÄÖÜa-zšžõäöü\-]+)*)")
                    names = name_regex.findall(line.replace(p_id, ""))
                    if not names and i > 0: names = name_regex.findall(lines[i-1])
                    if names:
                        name = max(names, key=len).strip()
                        info["unmasked_ids"][name] = p_id; info["unmasked_ids"][name.upper()] = p_id
        return info
    except Exception as e: logger.error(f"Error parsing PDF: {e}"); return {}

def iter_json_array(path):
    if shutil.which("jq"):
        proc = subprocess.Popen(["jq", "-c", ".[]", str(path)], stdout=subprocess.PIPE)
        for line in proc.stdout: yield json.loads(line)
    else:
        with open(path, 'r', encoding='utf-8') as f: yield from json.load(f)

class Downloader:
    def __init__(self, ddir, files):
        self.ddir = ddir; self.files = files; self.base = "https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/"
    def run(self):
        threads = [Thread(target=self._dl, args=(f,)) for f in self.files]
        for t in threads: t.start()
        for t in threads: t.join()
        return True
    def _dl(self, f):
        path = self.ddir / f; url = self.base + f
        try:
            req = urllib.request.Request(url, method='HEAD')
            with urllib.request.urlopen(req) as r: total = int(r.headers.get('content-length', 0))
            curr = path.stat().st_size if path.exists() else 0
            if curr >= total: return
            req = urllib.request.Request(url, headers={'Range': f'bytes={curr}-'})
            with urllib.request.urlopen(req) as r, open(path, 'ab') as out: shutil.copyfileobj(r, out)
            logger.info(f"Finished {f}")
        except Exception as e: logger.error(f"Error {f}: {e}")

# ============================================================
# Beautiful Display Logic
# ============================================================

def display_company(item, sections=None, lang="et"):
    lbl = UI_LABELS[lang]; to_en = (lang == "en")
    if sections is None or "all" in sections:
        sections = ["core", "general", "history", "personnel", "ownership", "beneficiaries", "operations", "registry", "enrichment"]
    yld = item.get('yldandmed', {}); enrich = item.get('enrichment', {}); unmasked_ids = enrich.get('unmasked_ids', {})
    def get_nested_list(key, inner_key=None):
        val = item.get(key, [])
        if not val: return []
        flat = []
        for entry in val:
            if isinstance(entry, dict) and inner_key and inner_key in entry: flat.extend(entry[inner_key])
            elif isinstance(entry, dict) and key in entry: flat.extend(entry[key])
            else: flat.append(entry)
        return flat
    def resolve_id(p):
        raw = p.get('isikukood_registrikood') or p.get('isikukood')
        if raw: return str(raw)
        name = f"{p.get('eesnimi', '')} {p.get('nimi_arinimi', '')}".strip() or p.get('isiku_nimi', '') or p.get('nimi', '')
        for lookup in [name.strip(), name.strip().upper()]:
            if lookup in unmasked_ids: return f"[bold green]{unmasked_ids[lookup]}[/bold green] [dim](unmasked)[/dim]"
        for u_n, u_id in unmasked_ids.items():
            if name.strip() in u_n or u_n in name.strip(): return f"[bold green]{u_id}[/bold green] [dim](unmasked)[/dim]"
        h = p.get('isikukood_hash'); return f"[dim]Hash: {h[:8]}...[/dim]" if h else "[dim]N/A[/dim]"

    n, c = item.get('nimi', 'N/A'), item.get('ariregistri_kood', 'N/A')
    console.print(f"\n[bold blue]█ {lbl['dossier']}: {n} ({c})[/bold blue]", style="header"); console.print("=" * console.width)
    if "core" in sections:
        t = Table(title=lbl["core"], box=box.ROUNDED, header_style="bold yellow", expand=True)
        t.add_column(lbl["attr"], style="cyan"); t.add_column(lbl["val"])
        for k, v in item.items():
            if not isinstance(v, (list, dict)): t.add_row(TRANSLATIONS.get(k, k) if to_en else k, str(translate_value(v, to_en)))
        t.add_row(lbl["portal_link"], f"https://ariregister.rik.ee/est/company/{c}"); console.print(t)
    if "enrichment" in sections and enrich:
        t = Table(title=lbl["enrichment"], box=box.ROUNDED, header_style="bold green", expand=True)
        t.add_column(lbl["attr"], style="green"); t.add_column(lbl["val"])
        for k, v in enrich.items():
            if k != "unmasked_ids": t.add_row(TRANSLATIONS.get(k, k) if to_en else k, str(v))
        t.add_row(lbl["unmasked_personal_ids"], f"{len(unmasked_ids)} {lbl['codes_unmasked']}"); console.print(t)
    if "general" in sections and yld:
        t = Table(title=lbl["general"], box=box.ROUNDED, header_style="bold yellow", expand=True)
        t.add_column(lbl["attr"], style="cyan"); t.add_column(lbl["val"])
        for k, v in yld.items():
            if not isinstance(v, (list, dict)): t.add_row(TRANSLATIONS.get(k, k) if to_en else k, str(translate_value(v, to_en)))
        console.print(t)
    if "history" in sections and yld:
        tree = Tree(f"[bold yellow]{lbl['history']}[/bold yellow]")
        lists = {"staatused": lbl["status_hist"], "arinimed": lbl["name_hist"], "aadressid": lbl["addr_hist"], "oiguslikud_vormid": lbl["legal_hist"], "kapitalid": lbl["cap_hist"], "oigusjargsused": lbl["mergers"], "markused_kaardil": lbl["annotations"]}
        for k, label in lists.items():
            data = yld.get(k, [])
            if data:
                node = tree.add(f"[cyan]{label}[/cyan]")
                for e in data: node.add(", ".join(f"[label]{TRANSLATIONS.get(key, key) if to_en else key}:[/label] {translate_value(val, to_en)}" for key, val in e.items() if not isinstance(val, (list, dict))))
        console.print(tree)
    if "personnel" in sections:
        board = get_nested_list('isikud', 'kaardile_kantud_isikud')
        if board:
            t = Table(title=lbl["personnel"], box=box.ROUNDED, header_style="bold yellow", expand=True)
            t.add_column(lbl["name"], style="bold white"); t.add_column(lbl["id_code"], style="magenta"); t.add_column(lbl["role"]); t.add_column(lbl["since"]); t.add_column(lbl["details"], style="dim")
            for p in board:
                t.add_row(f"{p.get('eesnimi', '')} {p.get('nimi_arinimi', '')}".strip(), resolve_id(p), translate_value(p.get('isiku_roll_tekstina', 'Member'), to_en), p.get('algus_kpv', ''), ", ".join([f"{TRANSLATIONS.get(k, k) if to_en else k}: {translate_value(v, to_en)}" for k, v in p.items() if k not in ["eesnimi", "nimi_arinimi", "isiku_roll_tekstina", "algus_kpv", "isikukood_registrikood", "isikukood_hash"] and not isinstance(v, (list, dict))]))
            console.print(t)
        r = get_nested_list('isikud', 'esindusoiguse_normaalregulatsioonid') + get_nested_list('isikud', 'esindusoiguse_eritingimused')
        if r: console.print(Panel("\n".join([f"• {translate_value(x.get('sisu'), to_en)}" for x in r]), title=lbl["rights"], box=box.ROUNDED, border_style="yellow"))
    if "ownership" in sections:
        sh = get_nested_list('osanikud', 'osanikud')
        if sh:
            t = Table(title=lbl["ownership"], box=box.ROUNDED, header_style="bold yellow", expand=True)
            t.add_column(lbl["owner"], style="bold white"); t.add_column(lbl["id_code"], style="magenta"); t.add_column(lbl["amount"], style="green"); t.add_column(lbl["type"]); t.add_column(lbl["details"], style="dim")
            for s in sh:
                amt = f"{s.get('osamaksu_summa') or s.get('osaluse_suurus') or '?'} {s.get('valuuta') or s.get('osaluse_valuuta') or 'EUR'}"
                t.add_row(f"{s.get('eesnimi', '')} {s.get('nimi_arinimi', '')}".strip() or "N/A", resolve_id(s), amt, translate_value(s.get('osaluse_omandiliik_tekstina', 'Owner'), to_en), ", ".join([f"{TRANSLATIONS.get(k, k) if to_en else k}: {translate_value(v, to_en)}" for k, v in s.items() if k not in ["eesnimi", "nimi_arinimi", "osamaksu_summa", "osaluse_suurus", "valuuta", "osaluse_valuuta", "osaluse_omandiliik_tekstina", "isikukood_registrikood", "isikukood_hash"] and not isinstance(v, (list, dict))]))
            console.print(t)
    if "beneficiaries" in sections:
        ben = get_nested_list('kasusaajad', 'kasusaajad')
        if ben:
            t = Table(title=lbl["beneficiaries"], box=box.ROUNDED, header_style="bold yellow", expand=True)
            t.add_column(lbl["name"], style="bold white"); t.add_column(lbl["id_code"], style="magenta"); t.add_column(lbl["control"]); t.add_column(lbl["address"])
            for b in ben: t.add_row(f"{b.get('eesnimi', '')} {b.get('nimi', '')}".strip() or "N/A", resolve_id(b), translate_value(b.get('kontrolli_teostamise_viis_tekstina', 'Control'), to_en), translate_value(b.get('aadress_riik_tekstina', 'N/A'), to_en))
            console.print(t)
    if "operations" in sections:
        a_l, r_l, side = yld.get('teatatud_tegevusalad', []), yld.get('info_majandusaasta_aruannetest', []), yld.get('sidevahendid', [])
        c1 = Table(title=lbl["activities"], box=box.SIMPLE); c1.add_column("Code"); c1.add_column(lbl["name"]); c1.add_column(lbl["main"])
        for a in a_l: c1.add_row(a.get('emtak_kood'), translate_value(a.get('emtak_tekstina'), to_en), "✓" if a.get('on_pohitegevusala') else "")
        c2 = Table(title=lbl["reports"], box=box.SIMPLE); c2.add_column(lbl["period_end"]); c2.add_column(lbl["employees"]); c2.add_column(lbl["activity"])
        for r in r_l: c2.add_row(r.get('majandusaasta_perioodi_lopp_kpv', 'N/A'), str(r.get('tootajate_arv', '0')), translate_value(r.get('tegevusala_emtak_tekstina', 'N/A'), to_en))
        c3 = Table(title=lbl["contacts"], box=box.SIMPLE); c3.add_column(lbl["type"]); c3.add_column(lbl["val"])
        for s in side: c3.add_row(translate_value(s.get('liik_tekstina', 'Contact'), to_en), s.get('sisu')); console.print(Columns([c1, c2, c3], expand=True))
    if "registry" in sections:
        t = Table(title=lbl["registry"], box=box.ROUNDED, header_style="bold yellow", expand=True)
        t.add_column(lbl["date"], style="cyan"); t.add_column(lbl["entry_type"]); t.add_column(lbl["entry_num"], justify="right")
        for card in get_nested_list('kaardid', 'registrikaardid'):
            for k in card.get('kanded', []): t.add_row(k.get('kpv'), translate_value(k.get('kandeliik_tekstina'), to_en), f"#{k.get('kande_nr')}")
        console.print(t)
    console.print(f"\n[dim]{lbl['privacy_note']}[/dim]")

# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Estonian Registry CLI")
    parser.add_argument("--no-db", action="store_true"); parser.add_argument("--force", action="store_true")
    parser.add_argument("--en", action="store_true"); parser.add_argument("--ee", action="store_true"); parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="cmd")
    for n, al in {"sync": ["sünk"], "merge": ["ühenda"]}.items(): sub.add_parser(n, aliases=al)
    sub.add_parser("enrich", aliases=["rikasta"]).add_argument("codes", nargs="+")
    sub.add_parser("export", aliases=["ekspordi"]).add_argument("output")
    srch = sub.add_parser("search", aliases=["otsi"])
    srch.add_argument("term", nargs="?"); srch.add_argument("-l", "--location"); srch.add_argument("-s", "--status"); srch.add_argument("-p", "--person")
    srch.add_argument("--json", action="store_true"); srch.add_argument("-t", "--translate", action="store_true"); srch.add_argument("--limit", type=int, default=5)
    for s in ["core", "general", "history", "personnel", "ownership", "beneficiaries", "operations", "registry", "enrichment"]:
        srch.add_argument(f"--{s}", action="append_const", dest="sections", const=s)
    args = parser.parse_args(); setup_logging(args.verbose); reg = EstonianRegistry(use_db=not args.no_db)
    
    et_cmds = ["otsi", "rikasta", "ühenda", "sünk", "ekspordi"]
    en_cmds = ["search", "enrich", "merge", "sync", "export"]
    cmd_typed = sys.argv[1] if len(sys.argv) > 1 else ""
    if args.en: lang = "en"
    elif args.ee: lang = "et"
    else: lang = "et" if (cmd_typed in et_cmds or (cmd_typed not in en_cmds and cmd_typed != "")) else "en"

    if args.cmd in ["sync", "sünk"]: reg.sync(force=args.force)
    elif args.cmd in ["merge", "ühenda"]: reg.merge(force=args.force)
    elif args.cmd in ["enrich", "rikasta"]: reg.enrich(args.codes)
    elif args.cmd in ["export", "ekspordi"]: reg.export(Path(args.output), translate=(args.translate or lang=="en"))
    elif args.cmd in ["search", "otsi"]:
        results = reg.db.search(term=args.term, person=args.person, location=args.location, status=args.status, limit=args.limit)
        sections, count = args.sections or ["all"], 0
        for item in results:
            count += 1
            if args.json: console.print(Syntax(json.dumps(translate_item(item, to_en=(args.translate or lang=="en")), indent=2, ensure_ascii=False), "json", theme="monokai"))
            else: display_company(item, sections=sections, lang=lang)
        if count == 0: console.print(f"[warning]{UI_LABELS[lang]['no_results']}[/warning]")
        else: console.print(f"\n[success]{UI_LABELS[lang]['results_found']}: {count}[/success]")

if __name__ == "__main__": main()
