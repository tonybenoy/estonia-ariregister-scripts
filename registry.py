#!/usr/bin/env python3
"""
Estonian Registry CLI - High Performance, Beautiful & Exhaustive
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

from difflib import get_close_matches

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

# Comprehensive Translation Map
TRANSLATIONS = {
    "ariregistri_kood": "registry_code",
    "nimi": "name",
    "staatus": "status",
    "aadress_maakond": "county",
    "aadress_linn": "city",
    "ettevotja_aadress": "address",
    "asukoht_ettevotja_aadressis": "location_in_address",
    "asukoha_ehak_kood": "location_ehak_code",
    "asukoha_ehak_tekstina": "location_description",
    "indeks_ettevotja_aadressis": "postal_code",
    "ettevotja_oiguslik_vorm": "legal_form",
    "ettevotja_oigusliku_vormi_alaliik": "legal_form_subtype",
    "ettevotja_staatus": "company_status",
    "ettevotja_staatus_tekstina": "status_description",
    "esmakande_kuupaev": "incorporation_date",
    "ettevotja_esmakande_kpv": "incorporation_date",
    "kustutamise_kuupaev": "deletion_date",
    "osanikud": "shareholders",
    "kasusaajad": "beneficiaries",
    "isikud": "persons",
    "kaardid": "registry_cards",
    "isikukood_registrikood": "id_code",
    "isikukood_hash": "id_hash",
    "nimi_arinimi": "name_legal_name",
    "isiku_roll": "role",
    "isiku_roll_tekstina": "role_description",
    "isiku_aadress": "person_address",
    "osathtede_arv": "shares_count",
    "osamaksu_summa": "contribution_amount",
    "valuuta": "currency",
    "osaluse_protsent": "ownership_percentage",
    "kontrolli_liik": "control_type",
    "yldandmed": "general_data",
    "kapitalid": "capital",
    "kapitali_suurus": "capital_amount",
    "kapitali_valuuta": "capital_currency",
    "sidevahendid": "contacts",
    "liik_tekstina": "type_description",
    "sisu": "content",
    "teatatud_tegevusalad": "activities",
    "emtak_kood": "emtak_code",
    "emtak_tekstina": "activity_description",
    "on_pohitegevusala": "is_main_activity",
    "info_majandusaasta_aruannetest": "annual_reports",
    "majandusaasta_perioodi_lopp_kpv": "period_end_date",
    "tootajate_arv": "employee_count",
    "registrikaardid": "registry_entries",
    "kanded": "entries",
    "kandeliik_tekstina": "entry_type",
    "kpv": "date",
    "kande_nr": "entry_number",
    "kaardi_piirkond": "card_region",
    "kaardi_nr": "card_number",
    "kaardi_tyyp": "card_type",
    "eesnimi": "first_name",
    "isiku_nimi": "person_name",
    "kontrolli_teostamise_viis_tekstina": "control_method",
    "aadress_riik_tekstina": "country",
    "ads_normaliseeritud_taisaadress": "normalized_address",
    "teabesysteemi_link": "portal_link",
    "algus_kpv": "start_date",
    "lopp_kpv": "end_date",
    "esindusoiguse_normaalregulatsioonid": "standard_representation_rights",
    "esindusoiguse_eritingimused": "special_representation_rights",
    "markused_kaardil": "registry_annotations",
    "oigusjargsused": "succession_mergers",
    "staatused": "status_history",
    "arinimed": "name_history",
    "aadressid": "address_history",
    "oiguslikud_vormid": "legal_form_history",
    "kapitalid": "capital_history",
    "enrichment": "enrichment",
    "processed_at": "processed_at",
    "unmasked_ids": "unmasked_ids",
    "tegutseb_tekstina": "is_active",
    "on_raamatupidamiskohustuslane": "is_accounting_obligated",
}

VALUE_TRANSLATIONS = {
    "Registrisse kantud": "Entered into register",
    "Kustutatud": "Deleted",
    "Likvideerimisel": "In liquidation",
    "Pankrotis": "Bankrupt",
    "Hoiatuskandega": "With warning entry",
    "Registrist kustutatud": "Deleted from register",
    "Osaühing": "Private limited company",
    "Aktsiaselts": "Public limited company",
    "Füüsilisest isikust ettevõtja": "Sole proprietor",
    "Mittetulundusühing": "Non-profit association",
    "Täisühing": "General partnership",
    "Usaldusühing": "Limited partnership",
    "Korteriühistu": "Apartment association",
    "Sihtasutus": "Foundation",
    "OÜ": "PLC",
    "AS": "JSC",
    "Juhatuse liige": "Management board member",
    "Osanik": "Shareholder",
    "Asutaja": "Founder",
    "Prokurist": "Proxy holder",
    "Likvideerija": "Liquidator",
    "Pankrotihaldur": "Trustee in bankruptcy",
    "Täisosanik": "General partner",
    "Usaldusosanik": "Limited partner",
    "otsene osalus": "direct ownership",
    "kaudne osalus": "indirect ownership",
    "muu kontrolli viis": "other form of control",
    "Esmakanne": "First entry",
    "Muutmiskanne": "Change entry",
    "Lõpetamiskanne": "Termination entry",
    "Tegevuse jätkamise kanne": "Continuation of activities entry",
    "Märkus": "Note",
    "Kapitali muutmise kanne": "Capital change entry",
    "Ärinime muutmise kanne": "Name change entry",
    "Asutamiskanne": "Founding entry",
    "Elektronposti aadress": "Email address",
    "Mobiiltelefon": "Mobile phone",
    "Telefon": "Telephone",
    "Interneti WWW aadress": "Website",
    "Faks": "Fax",
    "Programmeerimine": "Programming",
    "Mootorsõidukite jaemüük": "Retail sale of motor vehicles",
    "Jah": "Yes",
    "Ei": "No",
    "Osaühingut võib kõikide tehingute tegemisel esindada iga juhatuse liige.": "The private limited company may be represented by any member of the management board in all transactions.",
}

# ============================================================
# Industry Mapping (English names -> EMTAK prefixes)
# ============================================================

INDUSTRY_MAP = {
    # IT & Technology
    "software": ["62"], "it": ["62"], "tech": ["62"], "programming": ["6201"],
    "consulting": ["6202", "7022"], "it consulting": ["6202"], "data processing": ["6311"],
    "hosting": ["6311"], "web": ["6312"], "telecom": ["61"], "telecommunications": ["61"],
    # Manufacturing
    "manufacturing": ["10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33"],
    "food manufacturing": ["10"], "beverages": ["11"], "textiles": ["13"], "clothing": ["14"],
    "wood": ["16"], "paper": ["17"], "printing": ["18"], "chemicals": ["20"],
    "pharmaceuticals": ["21"], "plastics": ["22"], "metals": ["24", "25"], "electronics": ["26"],
    "machinery": ["28"], "automotive": ["29"], "furniture": ["31"],
    # Construction
    "construction": ["41", "42", "43"], "building": ["41"], "civil engineering": ["42"],
    "renovation": ["43"], "plumbing": ["4322"], "electrical": ["4321"],
    # Trade & Retail
    "retail": ["47"], "wholesale": ["46"], "trade": ["45", "46", "47"],
    "e-commerce": ["4791"], "car sales": ["45"], "grocery": ["4711"],
    # Food & Hospitality
    "restaurant": ["56"], "restaurants": ["56"], "food": ["56"], "catering": ["5621"],
    "hotel": ["55"], "hotels": ["55"], "accommodation": ["55"], "hospitality": ["55", "56"],
    "bar": ["5630"], "cafe": ["5610"],
    # Transport & Logistics
    "transport": ["49", "50", "51", "52"], "logistics": ["52"], "warehousing": ["5210"],
    "trucking": ["4941"], "freight": ["4941"], "taxi": ["4932"], "courier": ["5320"],
    "shipping": ["50"], "aviation": ["51"],
    # Real Estate
    "real estate": ["68"], "property": ["68"], "rental": ["6820"],
    # Finance & Insurance
    "finance": ["64", "65", "66"], "banking": ["6419"], "insurance": ["65"],
    "investment": ["6430"], "fintech": ["6419", "6499"],
    # Professional Services
    "legal": ["6910"], "law": ["6910"], "accounting": ["6920"], "audit": ["6920"],
    "management consulting": ["7022"], "architecture": ["7111"], "engineering": ["7112"],
    "design": ["7410"], "advertising": ["7311"], "marketing": ["7311", "7312"],
    "research": ["72"], "translation": ["7430"],
    # Healthcare
    "healthcare": ["86"], "medical": ["86"], "dental": ["8623"], "pharmacy": ["4773"],
    "veterinary": ["75"],
    # Education
    "education": ["85"], "training": ["8559"], "school": ["8520"],
    # Agriculture
    "agriculture": ["01"], "farming": ["01"], "forestry": ["02"], "fishing": ["03"],
    # Energy & Mining
    "energy": ["35"], "electricity": ["3511"], "mining": ["05", "06", "07", "08", "09"],
    "oil": ["06"], "gas": ["0620"],
    # Media & Entertainment
    "media": ["58", "59", "60"], "publishing": ["58"], "film": ["59"], "tv": ["60"],
    "gaming": ["5821"], "music": ["5920"],
    # Other Services
    "cleaning": ["8121", "8122"], "security": ["80"], "staffing": ["78"],
    "recruitment": ["7810"], "beauty": ["9602"], "hairdressing": ["9602"],
    "fitness": ["9311", "9313"], "sports": ["93"],
    "waste": ["38"], "recycling": ["3831", "3832"],
    "ngo": ["9412", "9499"], "nonprofit": ["9412", "9499"],
}

def resolve_industry(name):
    """Resolve an industry name or EMTAK code to a list of EMTAK prefixes."""
    if not name:
        return None
    name = name.strip()
    # If it looks like an EMTAK code already, return it directly
    if name.isdigit():
        return [name]
    key = name.lower()
    if key in INDUSTRY_MAP:
        return INDUSTRY_MAP[key]
    # Try close matches
    matches = get_close_matches(key, INDUSTRY_MAP.keys(), n=3, cutoff=0.6)
    if matches:
        console.print(f"[warning]Unknown industry '{name}'. Did you mean: {', '.join(matches)}?[/warning]")
        return None
    console.print(f"[warning]Unknown industry '{name}'. Use --list-industries to see available options.[/warning]")
    return None

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
        "analysis_title": "Analüüs", "rank": "Nr", "group": "Grupp", "count": "Arv", "pct": "Osakaal",
        "analysis_by": {"county": "maakond", "status": "staatus", "legal-form": "õiguslik vorm", "emtak": "EMTAK kood", "year": "asutamisaasta",
                        "capital-range": "kapitalivahemik", "employee-range": "tootajate vahemik", "role": "roll", "country": "riik"},
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
        "analysis_title": "Analysis", "rank": "Rank", "group": "Group", "count": "Count", "pct": "Share",
        "analysis_by": {"county": "County", "status": "Status", "legal-form": "Legal Form", "emtak": "EMTAK Code", "year": "Founding Year",
                        "capital-range": "Capital Range", "employee-range": "Employee Range", "role": "Person Role", "country": "Beneficiary Country"},
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
        self.conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
        self.conn.execute("PRAGMA journal_mode=WAL")
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
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_legal_form ON companies(legal_form)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON companies(status)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_founded_at ON companies(founded_at)")
            # New columns (Phase 1)
            existing = {r[1] for r in self.conn.execute("PRAGMA table_info(companies)").fetchall()}
            new_cols = [
                ("capital", "REAL"), ("capital_currency", "TEXT"), ("email", "TEXT"),
                ("phone", "TEXT"), ("website", "TEXT"), ("employee_count", "INTEGER"),
                ("vat_number", "TEXT"),
            ]
            for col, ctype in new_cols:
                if col not in existing:
                    self.conn.execute(f"ALTER TABLE companies ADD COLUMN {col} {ctype}")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_capital ON companies(capital)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_employee_count ON companies(employee_count)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_vat_number ON companies(vat_number)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_email ON companies(email)")
            # Persons denormalization table
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS persons (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_code INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    first_name TEXT, last_name TEXT, full_name TEXT,
                    id_code TEXT, id_hash TEXT,
                    role TEXT, start_date TEXT, end_date TEXT,
                    ownership_pct REAL, contribution_amount REAL, currency TEXT,
                    country TEXT,
                    FOREIGN KEY (company_code) REFERENCES companies(code)
                )
            """)
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_persons_name ON persons(full_name)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_persons_id_code ON persons(id_code)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_persons_company ON persons(company_code)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_persons_source ON persons(source)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_persons_role ON persons(role)")

    @staticmethod
    def _normalize_date(date_str):
        if not date_str: return None
        parts = date_str.strip().split('.')
        if len(parts) == 3 and len(parts[2]) == 4:
            return f"{parts[2]}-{parts[1]}-{parts[0]}"
        return date_str

    @staticmethod
    def _extract_county(item):
        ehak = item.get('asukoha_ehak_tekstina', '') or ''
        parts = [p.strip() for p in ehak.split(',')]
        for p in reversed(parts):
            if 'maakond' in p: return p
        return parts[-1] if parts and parts[-1] else None

    @staticmethod
    def _extract_city(item):
        ehak = item.get('asukoha_ehak_tekstina', '') or ''
        parts = [p.strip() for p in ehak.split(',')]
        for p in parts:
            if 'linn' in p or 'vald' in p: return p
        return parts[1] if len(parts) > 1 else (parts[0] if parts and parts[0] else None)

    @staticmethod
    def _extract_latest_capital(item):
        caps = item.get('yldandmed', {}).get('kapitalid', [])
        if not caps:
            return None, None
        best = None
        for c in caps:
            if not c.get('lopp_kpv'):
                if best is None or (c.get('algus_kpv', '') > best.get('algus_kpv', '')):
                    best = c
        if not best:
            best = max(caps, key=lambda c: c.get('algus_kpv', ''))
        amt = best.get('kapitali_suurus')
        cur = best.get('kapitali_valuuta', 'EUR')
        try:
            return float(amt), cur
        except (TypeError, ValueError):
            return None, None

    @staticmethod
    def _extract_contacts(item):
        contacts = item.get('yldandmed', {}).get('sidevahendid', [])
        email = phone = website = None
        for c in contacts:
            ctype = (c.get('liik_tekstina') or '').lower()
            val = c.get('sisu', '')
            if not val:
                continue
            if not email and ('post' in ctype or 'mail' in ctype):
                email = val
            elif not phone and ('telefon' in ctype or 'mobiil' in ctype):
                phone = val
            elif not website and ('www' in ctype or 'internet' in ctype):
                website = val
        return email, phone, website

    @staticmethod
    def _extract_latest_employees(item):
        reports = item.get('yldandmed', {}).get('info_majandusaasta_aruannetest', [])
        if not reports:
            return None
        sorted_reports = sorted(reports, key=lambda r: r.get('majandusaasta_perioodi_lopp_kpv', ''), reverse=True)
        for r in sorted_reports:
            emp = r.get('tootajate_arv')
            if emp is not None:
                try:
                    return int(emp)
                except (TypeError, ValueError):
                    pass
        return None

    def insert_batch_base(self, batch):
        with self.conn:
            self.conn.executemany(
                """INSERT OR REPLACE INTO companies
                   (code, name, status, maakond, linn, legal_form, founded_at, full_data, vat_number)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [(i.get('ariregistri_kood'), i.get('nimi'), i.get('ettevotja_staatus_tekstina'),
                  self._extract_county(i), self._extract_city(i),
                  i.get('ettevotja_oiguslik_vorm'), self._normalize_date(i.get('ettevotja_esmakande_kpv')),
                  json.dumps(i), i.get('kmkr_nr') or None) for i in batch]
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
                if not code: continue
                self.conn.execute("UPDATE companies SET full_data = json_patch(full_data, ?) WHERE code = ?", (json.dumps(item), code))
                updates = []; params = []
                status = item.get('staatus_tekstina')
                if status: updates.append("status = COALESCE(?, status)"); params.append(status)
                founded = self._normalize_date(item.get('esmaregistreerimise_kpv'))
                if founded: updates.append("founded_at = COALESCE(?, founded_at)"); params.append(founded)
                # Extract new derived columns
                cap_amt, cap_cur = self._extract_latest_capital(item)
                if cap_amt is not None:
                    updates.append("capital = ?"); params.append(cap_amt)
                    updates.append("capital_currency = ?"); params.append(cap_cur)
                email, phone, website = self._extract_contacts(item)
                if email: updates.append("email = ?"); params.append(email)
                if phone: updates.append("phone = ?"); params.append(phone)
                if website: updates.append("website = ?"); params.append(website)
                emp = self._extract_latest_employees(item)
                if emp is not None: updates.append("employee_count = ?"); params.append(emp)
                if updates:
                    params.append(code)
                    self.conn.execute(f"UPDATE companies SET {', '.join(updates)} WHERE code = ?", params)

    def update_enrichment(self, code: int, enrichment: dict):
        with self.conn: self.conn.execute("UPDATE companies SET enrichment = ? WHERE code = ?", (json.dumps(enrichment), code))

    def search(self, term=None, person=None, location=None, status=None, limit=None,
               emtak=None, founded_after=None, founded_before=None, legal_form=None,
               min_capital=None, max_capital=None, has_email=False, has_phone=False, has_website=False):
        query = "SELECT * FROM companies WHERE 1=1"; params = []
        if term:
            if term.isdigit(): query += " AND code = ?"; params.append(int(term))
            else: query += " AND name LIKE ?"; params.append(f"%{term}%")
        if location: query += " AND (maakond LIKE ? OR linn LIKE ?)"; params.extend([f"%{location}%", f"%{location}%"])
        if status: query += " AND (status LIKE ? OR full_data LIKE ?)"; params.extend([f"%{status}%", f"%{status}%"])
        if person: query += " AND (full_data LIKE ? OR enrichment LIKE ?)"; params.extend([f"%{person}%", f"%{person}%"])
        if emtak:
            if isinstance(emtak, list):
                placeholders = " OR ".join(["json_extract(e.value, '$.emtak_kood') LIKE ?"] * len(emtak))
                query += f""" AND EXISTS (SELECT 1 FROM json_each(companies.full_data, '$.yldandmed.teatatud_tegevusalad') AS e
                             WHERE {placeholders})"""
                params.extend([f"{e}%" for e in emtak])
            else:
                query += """ AND EXISTS (SELECT 1 FROM json_each(companies.full_data, '$.yldandmed.teatatud_tegevusalad') AS e
                             WHERE json_extract(e.value, '$.emtak_kood') LIKE ?)"""
                params.append(f"{emtak}%")
        if founded_after: query += " AND founded_at >= ?"; params.append(founded_after)
        if founded_before: query += " AND founded_at <= ?"; params.append(founded_before)
        if legal_form: query += " AND legal_form LIKE ?"; params.append(f"%{legal_form}%")
        if min_capital is not None: query += " AND capital >= ?"; params.append(float(min_capital))
        if max_capital is not None: query += " AND capital <= ?"; params.append(float(max_capital))
        if has_email: query += " AND email IS NOT NULL"
        if has_phone: query += " AND phone IS NOT NULL"
        if has_website: query += " AND website IS NOT NULL"
        if limit: query += f" LIMIT {int(limit)}"
        for row in self.conn.execute(query, params):
            data = json.loads(row['full_data'])
            if row['enrichment']: data['enrichment'] = json.loads(row['enrichment'])
            yield data

    def analyze(self, by, emtak=None, location=None, status=None, legal_form=None,
                founded_after=None, founded_before=None, top=20):
        where_clauses = []; params = []
        if emtak:
            if isinstance(emtak, list):
                placeholders = " OR ".join(["json_extract(e.value, '$.emtak_kood') LIKE ?"] * len(emtak))
                where_clauses.append(f"""EXISTS (SELECT 1 FROM json_each(c.full_data, '$.yldandmed.teatatud_tegevusalad') AS e
                                    WHERE {placeholders})""")
                params.extend([f"{e}%" for e in emtak])
            else:
                where_clauses.append("""EXISTS (SELECT 1 FROM json_each(c.full_data, '$.yldandmed.teatatud_tegevusalad') AS e
                                    WHERE json_extract(e.value, '$.emtak_kood') LIKE ?)""")
                params.append(f"{emtak}%")
        if location: where_clauses.append("(c.maakond LIKE ? OR c.linn LIKE ?)"); params.extend([f"%{location}%", f"%{location}%"])
        if status: where_clauses.append("c.status LIKE ?"); params.append(f"%{status}%")
        if legal_form: where_clauses.append("c.legal_form LIKE ?"); params.append(f"%{legal_form}%")
        if founded_after: where_clauses.append("c.founded_at >= ?"); params.append(founded_after)
        if founded_before: where_clauses.append("c.founded_at <= ?"); params.append(founded_before)
        where_sql = (" AND " + " AND ".join(where_clauses)) if where_clauses else ""

        if by == "county":
            query = f"SELECT COALESCE(c.maakond, 'Unknown') AS grp, COUNT(*) AS cnt FROM companies c WHERE 1=1{where_sql} GROUP BY grp ORDER BY cnt DESC LIMIT ?"
        elif by == "status":
            query = f"SELECT COALESCE(c.status, 'Unknown') AS grp, COUNT(*) AS cnt FROM companies c WHERE 1=1{where_sql} GROUP BY grp ORDER BY cnt DESC LIMIT ?"
        elif by == "legal-form":
            query = f"SELECT COALESCE(c.legal_form, 'Unknown') AS grp, COUNT(*) AS cnt FROM companies c WHERE 1=1{where_sql} GROUP BY grp ORDER BY cnt DESC LIMIT ?"
        elif by == "year":
            query = f"SELECT COALESCE(SUBSTR(c.founded_at, 1, 4), 'Unknown') AS grp, COUNT(*) AS cnt FROM companies c WHERE 1=1{where_sql} GROUP BY grp ORDER BY grp ASC LIMIT ?"
        elif by == "emtak":
            emtak_filter = ""
            if emtak:
                if isinstance(emtak, list):
                    emtak_filter = " AND (" + " OR ".join(["json_extract(items.value, '$.emtak_kood') LIKE ?"] * len(emtak)) + ")"
                    params.extend([f"{e}%" for e in emtak])
                else:
                    emtak_filter = " AND json_extract(items.value, '$.emtak_kood') LIKE ?"
                    params.append(f"{emtak}%")
            query = f"""SELECT json_extract(items.value, '$.emtak_kood') || ' - ' || COALESCE(json_extract(items.value, '$.emtak_tekstina'), '?') AS grp, COUNT(*) AS cnt
                FROM companies c, json_each(c.full_data, '$.yldandmed.teatatud_tegevusalad') AS items
                WHERE json_extract(items.value, '$.emtak_kood') IS NOT NULL{emtak_filter}{where_sql}
                GROUP BY json_extract(items.value, '$.emtak_kood') ORDER BY cnt DESC LIMIT ?"""
        elif by == "capital-range":
            query = f"""SELECT CASE
                WHEN c.capital IS NULL THEN 'No data'
                WHEN c.capital < 2500 THEN '< 2,500'
                WHEN c.capital < 25000 THEN '2,500 - 25K'
                WHEN c.capital < 100000 THEN '25K - 100K'
                WHEN c.capital < 1000000 THEN '100K - 1M'
                ELSE '1M+' END AS grp, COUNT(*) AS cnt
                FROM companies c WHERE 1=1{where_sql} GROUP BY grp ORDER BY
                CASE grp WHEN 'No data' THEN 0 WHEN '< 2,500' THEN 1 WHEN '2,500 - 25K' THEN 2
                WHEN '25K - 100K' THEN 3 WHEN '100K - 1M' THEN 4 WHEN '1M+' THEN 5 END LIMIT ?"""
        elif by == "employee-range":
            query = f"""SELECT CASE
                WHEN c.employee_count IS NULL THEN 'No data'
                WHEN c.employee_count = 0 THEN '0'
                WHEN c.employee_count <= 5 THEN '1-5'
                WHEN c.employee_count <= 20 THEN '6-20'
                WHEN c.employee_count <= 100 THEN '21-100'
                WHEN c.employee_count <= 500 THEN '101-500'
                ELSE '500+' END AS grp, COUNT(*) AS cnt
                FROM companies c WHERE 1=1{where_sql} GROUP BY grp ORDER BY
                CASE grp WHEN 'No data' THEN 0 WHEN '0' THEN 1 WHEN '1-5' THEN 2 WHEN '6-20' THEN 3
                WHEN '21-100' THEN 4 WHEN '101-500' THEN 5 WHEN '500+' THEN 6 END LIMIT ?"""
        elif by == "role":
            query = f"""SELECT COALESCE(p.role, 'Unknown') AS grp, COUNT(DISTINCT p.company_code) AS cnt
                FROM persons p JOIN companies c ON p.company_code = c.code WHERE 1=1{where_sql}
                GROUP BY grp ORDER BY cnt DESC LIMIT ?"""
        elif by == "country":
            query = f"""SELECT COALESCE(p.country, 'Unknown') AS grp, COUNT(DISTINCT p.company_code) AS cnt
                FROM persons p JOIN companies c ON p.company_code = c.code WHERE p.source = 'beneficiary'{where_sql}
                GROUP BY grp ORDER BY cnt DESC LIMIT ?"""
        else:
            return []
        params.append(top)
        return [(row[0], row[1]) for row in self.conn.execute(query, params)]

    def is_file_processed(self, filename: str):
        return self.conn.execute("SELECT 1 FROM sync_state WHERE filename=? AND status='DONE'", (filename,)).fetchone() is not None

    def mark_file_status(self, filename: str, status: str):
        with self.conn: self.conn.execute("INSERT OR REPLACE INTO sync_state VALUES (?, ?)", (filename, status))

    def get_stats(self):
        total = self.conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
        enriched = self.conn.execute("SELECT COUNT(*) FROM companies WHERE enrichment IS NOT NULL").fetchone()[0]
        has_status = self.conn.execute("SELECT COUNT(*) FROM companies WHERE status IS NOT NULL").fetchone()[0]
        has_county = self.conn.execute("SELECT COUNT(*) FROM companies WHERE maakond IS NOT NULL").fetchone()[0]
        has_founded = self.conn.execute("SELECT COUNT(*) FROM companies WHERE founded_at IS NOT NULL").fetchone()[0]
        has_emtak = self.conn.execute("SELECT COUNT(*) FROM companies WHERE full_data LIKE '%emtak_kood%'").fetchone()[0]
        has_capital = self.conn.execute("SELECT COUNT(*) FROM companies WHERE capital IS NOT NULL").fetchone()[0]
        has_email = self.conn.execute("SELECT COUNT(*) FROM companies WHERE email IS NOT NULL").fetchone()[0]
        has_phone = self.conn.execute("SELECT COUNT(*) FROM companies WHERE phone IS NOT NULL").fetchone()[0]
        has_website = self.conn.execute("SELECT COUNT(*) FROM companies WHERE website IS NOT NULL").fetchone()[0]
        has_employees = self.conn.execute("SELECT COUNT(*) FROM companies WHERE employee_count IS NOT NULL").fetchone()[0]
        has_vat = self.conn.execute("SELECT COUNT(*) FROM companies WHERE vat_number IS NOT NULL").fetchone()[0]
        persons_count = 0
        try:
            persons_count = self.conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
        except sqlite3.OperationalError:
            pass
        top_counties = self.conn.execute("SELECT maakond, COUNT(*) AS cnt FROM companies WHERE maakond IS NOT NULL GROUP BY maakond ORDER BY cnt DESC LIMIT 5").fetchall()
        top_legal = self.conn.execute("SELECT legal_form, COUNT(*) AS cnt FROM companies WHERE legal_form IS NOT NULL GROUP BY legal_form ORDER BY cnt DESC LIMIT 5").fetchall()
        return {
            "total": total, "enriched": enriched,
            "has_status": has_status, "has_county": has_county, "has_founded": has_founded, "has_emtak": has_emtak,
            "has_capital": has_capital, "has_email": has_email, "has_phone": has_phone, "has_website": has_website,
            "has_employees": has_employees, "has_vat": has_vat, "persons_count": persons_count,
            "top_counties": [(r[0], r[1]) for r in top_counties],
            "top_legal": [(r[0], r[1]) for r in top_legal],
        }

    def search_persons(self, name=None, id_code=None, role=None, source=None, company_code=None, limit=50):
        query = """SELECT p.*, c.name AS company_name FROM persons p
                   JOIN companies c ON p.company_code = c.code WHERE 1=1"""
        params = []
        if name: query += " AND p.full_name LIKE ?"; params.append(f"%{name}%")
        if id_code: query += " AND p.id_code = ?"; params.append(str(id_code))
        if role: query += " AND p.role LIKE ?"; params.append(f"%{role}%")
        if source: query += " AND p.source = ?"; params.append(source)
        if company_code: query += " AND p.company_code = ?"; params.append(int(company_code))
        query += " ORDER BY p.full_name"
        if limit: query += f" LIMIT {int(limit)}"
        return [dict(row) for row in self.conn.execute(query, params)]

    def person_network(self, name=None, id_code=None):
        query = """SELECT p.*, c.name AS company_name, c.status AS company_status FROM persons p
                   JOIN companies c ON p.company_code = c.code WHERE """
        params = []
        if id_code:
            query += "p.id_code = ?"; params.append(str(id_code))
        elif name:
            query += "p.full_name LIKE ?"; params.append(f"%{name}%")
        else:
            return []
        query += " ORDER BY p.source, c.name"
        return [dict(row) for row in self.conn.execute(query, params)]

    def find_group(self, code, direction="both", max_depth=5):
        results = {"company": None, "parents": [], "subsidiaries": []}
        # Get the root company
        row = self.conn.execute("SELECT code, name, status FROM companies WHERE code = ?", (int(code),)).fetchone()
        if not row: return results
        results["company"] = dict(row)

        if direction in ("up", "both"):
            # Find parent shareholders (who owns this company)
            parents = self.conn.execute(
                """SELECT p.*, c.name AS company_name FROM persons p
                   JOIN companies c ON p.company_code = c.code
                   WHERE p.company_code = ? AND p.source = 'shareholder'""",
                (int(code),)).fetchall()
            results["parents"] = [dict(r) for r in parents]

        if direction in ("down", "both"):
            # Find subsidiaries (where this code appears as shareholder id_code)
            visited = set()
            queue = [(int(code), 0)]
            while queue:
                current_code, depth = queue.pop(0)
                if depth >= max_depth or current_code in visited:
                    continue
                visited.add(current_code)
                subs = self.conn.execute(
                    """SELECT DISTINCT p.company_code, c.name AS company_name, c.status,
                              p.ownership_pct, p.contribution_amount, p.currency
                       FROM persons p JOIN companies c ON p.company_code = c.code
                       WHERE p.id_code = ? AND p.source = 'shareholder'""",
                    (str(current_code),)).fetchall()
                for s in subs:
                    sub = dict(s)
                    sub["depth"] = depth + 1
                    sub["parent_code"] = current_code
                    results["subsidiaries"].append(sub)
                    queue.append((sub["company_code"], depth + 1))
        return results

    def employee_trend(self, code=None, emtak=None, location=None):
        if code:
            row = self.conn.execute("SELECT full_data FROM companies WHERE code = ?", (int(code),)).fetchone()
            if not row: return []
            data = json.loads(row[0])
            reports = data.get('yldandmed', {}).get('info_majandusaasta_aruannetest', [])
            trend = []
            for r in sorted(reports, key=lambda x: x.get('majandusaasta_perioodi_lopp_kpv', '')):
                emp = r.get('tootajate_arv')
                if emp is not None:
                    try:
                        trend.append({"year": r.get('majandusaasta_perioodi_lopp_kpv', '')[:4], "employees": int(emp)})
                    except (TypeError, ValueError):
                        pass
            return trend
        else:
            # Industry-wide aggregation
            where_clauses = []; params = []
            if emtak:
                if isinstance(emtak, list):
                    placeholders = " OR ".join(["json_extract(e2.value, '$.emtak_kood') LIKE ?"] * len(emtak))
                    where_clauses.append(f"""EXISTS (SELECT 1 FROM json_each(c.full_data, '$.yldandmed.teatatud_tegevusalad') AS e2
                                        WHERE {placeholders})""")
                    params.extend([f"{e}%" for e in emtak])
                else:
                    where_clauses.append("""EXISTS (SELECT 1 FROM json_each(c.full_data, '$.yldandmed.teatatud_tegevusalad') AS e2
                                        WHERE json_extract(e2.value, '$.emtak_kood') LIKE ?)""")
                    params.append(f"{emtak}%")
            if location:
                where_clauses.append("(c.maakond LIKE ? OR c.linn LIKE ?)")
                params.extend([f"%{location}%", f"%{location}%"])
            where_sql = (" AND " + " AND ".join(where_clauses)) if where_clauses else ""
            query = f"""SELECT SUBSTR(json_extract(r.value, '$.majandusaasta_perioodi_lopp_kpv'), 1, 4) AS yr,
                               SUM(CAST(json_extract(r.value, '$.tootajate_arv') AS INTEGER)) AS total_emp,
                               COUNT(DISTINCT c.code) AS company_count
                        FROM companies c, json_each(c.full_data, '$.yldandmed.info_majandusaasta_aruannetest') AS r
                        WHERE json_extract(r.value, '$.tootajate_arv') IS NOT NULL{where_sql}
                        GROUP BY yr ORDER BY yr"""
            return [{"year": r[0], "employees": r[1], "companies": r[2]} for r in self.conn.execute(query, params)]

    def populate_persons(self):
        logger.info("Populating persons table...")
        with self.conn:
            self.conn.execute("DELETE FROM persons")
            cursor = self.conn.execute("SELECT code, full_data FROM companies")
            batch = []; count = 0
            for row in cursor:
                code = row[0]
                data = json.loads(row[1])
                # Board members from kaardile_kantud_isikud
                for group in data.get('isikud', []):
                    if isinstance(group, dict):
                        for p in group.get('kaardile_kantud_isikud', []):
                            first = p.get('eesnimi', '')
                            last = p.get('nimi_arinimi', '')
                            full = f"{first} {last}".strip()
                            batch.append((code, 'board', first or None, last or None, full or None,
                                         str(p.get('isikukood_registrikood', '')) or None,
                                         p.get('isikukood_hash'),
                                         p.get('isiku_roll_tekstina'),
                                         p.get('algus_kpv'), p.get('lopp_kpv'),
                                         None, None, None, None))
                # Shareholders from osanikud
                for group in data.get('osanikud', []):
                    if isinstance(group, dict):
                        for s in group.get('osanikud', []):
                            first = s.get('eesnimi', '')
                            last = s.get('nimi_arinimi', '')
                            full = f"{first} {last}".strip()
                            pct = s.get('osaluse_protsent')
                            amt = s.get('osamaksu_summa') or s.get('osaluse_suurus')
                            cur = s.get('valuuta') or s.get('osaluse_valuuta')
                            try: pct = float(pct) if pct else None
                            except (TypeError, ValueError): pct = None
                            try: amt = float(amt) if amt else None
                            except (TypeError, ValueError): amt = None
                            batch.append((code, 'shareholder', first or None, last or None, full or None,
                                         str(s.get('isikukood_registrikood', '')) or None,
                                         s.get('isikukood_hash'),
                                         s.get('osaluse_omandiliik_tekstina'),
                                         s.get('algus_kpv'), s.get('lopp_kpv'),
                                         pct, amt, cur, None))
                # Beneficiaries from kasusaajad
                for group in data.get('kasusaajad', []):
                    if isinstance(group, dict):
                        for b in group.get('kasusaajad', []):
                            first = b.get('eesnimi', '')
                            last = b.get('nimi', '')
                            full = f"{first} {last}".strip()
                            batch.append((code, 'beneficiary', first or None, last or None, full or None,
                                         str(b.get('isikukood_registrikood', '')) or None,
                                         b.get('isikukood_hash'),
                                         b.get('kontrolli_teostamise_viis_tekstina'),
                                         None, None, None, None, None,
                                         b.get('aadress_riik_tekstina')))
                count += 1
                if len(batch) >= 10000:
                    self.conn.executemany(
                        """INSERT INTO persons (company_code, source, first_name, last_name, full_name,
                           id_code, id_hash, role, start_date, end_date,
                           ownership_pct, contribution_amount, currency, country) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", batch)
                    batch = []
            if batch:
                self.conn.executemany(
                    """INSERT INTO persons (company_code, source, first_name, last_name, full_name,
                       id_code, id_hash, role, start_date, end_date,
                       ownership_pct, contribution_amount, currency, country) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", batch)
        total = self.conn.execute("SELECT COUNT(*) FROM persons").fetchone()[0]
        logger.info(f"Populated {total:,} person records from {count:,} companies")

    def rebuild_derived_columns(self):
        logger.info("Rebuilding derived columns from full_data...")
        cursor = self.conn.execute("SELECT code, full_data FROM companies")
        count = 0
        with self.conn:
            for row in cursor:
                code = row[0]
                data = json.loads(row[1])
                updates = []; params = []
                cap_amt, cap_cur = self._extract_latest_capital(data)
                if cap_amt is not None:
                    updates.append("capital = ?"); params.append(cap_amt)
                    updates.append("capital_currency = ?"); params.append(cap_cur)
                email, phone, website = self._extract_contacts(data)
                if email: updates.append("email = ?"); params.append(email)
                if phone: updates.append("phone = ?"); params.append(phone)
                if website: updates.append("website = ?"); params.append(website)
                emp = self._extract_latest_employees(data)
                if emp is not None: updates.append("employee_count = ?"); params.append(emp)
                if updates:
                    params.append(code)
                    self.conn.execute(f"UPDATE companies SET {', '.join(updates)} WHERE code = ?", params)
                count += 1
                if count % 50000 == 0:
                    logger.info(f"  Processed {count:,} companies...")
        logger.info(f"Rebuilt derived columns for {count:,} companies")

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

    def __init__(self, data_dir="data", chunk_size=50000, backend: RegistryBackend = None, use_db=True):
        self.data_dir = Path(data_dir); self.download_dir = self.data_dir / "downloads"
        self.extracted_dir = self.data_dir / "extracted"; self.db_path = self.data_dir / "registry.db"
        for d in [self.download_dir, self.extracted_dir]: d.mkdir(parents=True, exist_ok=True)
        self.chunk_size = chunk_size
        if not use_db: self.db = None
        else: self.db = backend or SQLiteBackend(self.db_path)

    def sync(self, force=False):
        Downloader(self.download_dir, self.DATA_FILES).run()
        self.merge(force=force)

    def merge(self, force=False):
        if not self.db: return
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
                        if len(batch) >= self.chunk_size:
                            self.db.insert_batch_base(batch); batch = []
                if batch: self.db.insert_batch_base(batch)
            elif 'yldandmed' in f:
                batch = []
                for item in iter_json_array(path):
                    if item.get('ariregistri_kood'):
                        item['ariregistri_kood'] = int(item['ariregistri_kood']); batch.append(item)
                    if len(batch) >= self.chunk_size:
                        self.db.update_batch_general(batch); batch = []
                if batch: self.db.update_batch_general(batch)
            else:
                key = f.split('__')[-1].split('.')[0]; groups = defaultdict(list); count = 0
                for item in iter_json_array(path):
                    code = item.get('ariregistri_kood')
                    if code: groups[int(code)].append(item.get(key, item)); count += 1
                    if count >= self.chunk_size:
                        self.db.update_batch_json(key, groups); groups = defaultdict(list); count = 0
                if groups: self.db.update_batch_json(key, groups)
            self.db.mark_file_status(f, 'DONE'); self.db.commit()
        if force:
            self.db.rebuild_derived_columns()
            self.db.populate_persons()
            self.db.commit()

    def enrich(self, codes: list[str]):
        if not self.db: return
        for code_str in codes:
            try:
                code = int(code_str); console.print(f"[info]Enriching {code}...[/info]")
                pdf_bytes = download_registry_pdf(str(code))
                if not pdf_bytes: continue
                self.db.update_enrichment(code, parse_pdf_content(pdf_bytes))
                console.print(f"[success]Updated {code}[/success]"); time.sleep(1)
            except Exception as e: logger.error(f"Error enriching {code_str}: {e}")

    def export(self, output_path: Path, translate: bool = False):
        if not self.db: return
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

def _convert_decimals(obj):
    from decimal import Decimal
    if isinstance(obj, Decimal): return float(obj) if obj % 1 else int(obj)
    if isinstance(obj, dict): return {k: _convert_decimals(v) for k, v in obj.items()}
    if isinstance(obj, list): return [_convert_decimals(v) for v in obj]
    return obj

def iter_json_array(path):
    if shutil.which("jq"):
        proc = subprocess.Popen(["jq", "-c", ".[]", str(path)], stdout=subprocess.PIPE)
        for line in proc.stdout: yield json.loads(line)
    else:
        import ijson
        with open(path, 'rb') as f:
            for item in ijson.items(f, 'item'):
                yield _convert_decimals(item)

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
    def format_dict(d, indent=4, exclude=None):
        exclude = exclude or []; res = []
        for k, v in d.items():
            if k in exclude or isinstance(v, (list, dict)): continue 
            res.append(f"{' '*indent}\033[1m{TRANSLATIONS.get(k, k) if to_en else k}:\033[0m {translate_value(v, to_en)}")
        return res
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
    console.print(f"\n[bold blue]== {lbl['dossier']}: {n} ({c}) ==[/bold blue]", style="header"); console.print("=" * console.width)
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
        for s in side: c3.add_row(translate_value(s.get('liik_tekstina', 'Contact'), to_en), s.get('sisu'))
        console.print(Columns([c1, c2, c3], expand=True))
    if "registry" in sections:
        t = Table(title=lbl["registry"], box=box.ROUNDED, header_style="bold yellow", expand=True)
        t.add_column(lbl["date"], style="cyan"); t.add_column(lbl["entry_type"]); t.add_column(lbl["entry_num"], justify="right")
        for card in get_nested_list('kaardid', 'registrikaardid'):
            for k in card.get('kanded', []): t.add_row(k.get('kpv'), translate_value(k.get('kandeliik_tekstina'), to_en), f"#{k.get('kande_nr')}")
        console.print(t)
    console.print(f"\n[dim]{lbl['privacy_note']}[/dim]")

def display_stats(stats, lang="et"):
    lbl = UI_LABELS[lang]; to_en = (lang == "en")
    title = lbl.get("stats_title", "Database Statistics" if to_en else "Andmebaasi statistika")
    t = Table(title=title, box=box.ROUNDED, header_style="bold yellow", expand=True)
    t.add_column(lbl["attr"], style="cyan"); t.add_column(lbl["val"], justify="right")
    total = stats["total"]
    def pct(n): return f"{n:,} ({n/total*100:.1f}%)" if total else "0"
    t.add_row("Total companies" if to_en else "Ettevotteid kokku", f"{total:,}")
    t.add_row("With status" if to_en else "Staatusega", pct(stats["has_status"]))
    t.add_row("With county" if to_en else "Maakonnaga", pct(stats["has_county"]))
    t.add_row("With founding date" if to_en else "Asutamiskuupaevaga", pct(stats["has_founded"]))
    t.add_row("With EMTAK codes" if to_en else "EMTAK koodidega", pct(stats["has_emtak"]))
    t.add_row("With capital data" if to_en else "Kapitaliga", pct(stats.get("has_capital", 0)))
    t.add_row("With email" if to_en else "E-postiga", pct(stats.get("has_email", 0)))
    t.add_row("With phone" if to_en else "Telefoniga", pct(stats.get("has_phone", 0)))
    t.add_row("With website" if to_en else "Veebilehega", pct(stats.get("has_website", 0)))
    t.add_row("With employee count" if to_en else "Tootajate arvuga", pct(stats.get("has_employees", 0)))
    t.add_row("With VAT number" if to_en else "KMKR numbriga", pct(stats.get("has_vat", 0)))
    t.add_row("Person records" if to_en else "Isikukirjeid", f"{stats.get('persons_count', 0):,}")
    t.add_row("PDF enriched" if to_en else "PDF-iga rikastatud", pct(stats["enriched"]))
    console.print(t)
    if stats["top_counties"]:
        t2 = Table(title="Top counties" if to_en else "Suurimad maakonnad", box=box.SIMPLE)
        t2.add_column(lbl.get("group", "Group"), style="cyan"); t2.add_column(lbl["count"], justify="right")
        for name, cnt in stats["top_counties"]:
            t2.add_row(translate_value(name, to_en) if name else "N/A", f"{cnt:,}")
        console.print(t2)
    if stats["top_legal"]:
        t3 = Table(title="Top legal forms" if to_en else "Levinumad oiguslikud vormid", box=box.SIMPLE)
        t3.add_column(lbl.get("group", "Group"), style="cyan"); t3.add_column(lbl["count"], justify="right")
        for name, cnt in stats["top_legal"]:
            t3.add_row(translate_value(name, to_en) if name else "N/A", f"{cnt:,}")
        console.print(t3)

def get_latest_employees(item):
    """Extract latest employee count from annual reports."""
    reports = item.get('yldandmed', {}).get('info_majandusaasta_aruannetest', [])
    if not reports:
        return None
    # Sort by period end date descending, take latest
    sorted_reports = sorted(reports, key=lambda r: r.get('majandusaasta_perioodi_lopp_kpv', ''), reverse=True)
    for r in sorted_reports:
        emp = r.get('tootajate_arv')
        if emp is not None:
            try:
                return int(emp)
            except (ValueError, TypeError):
                pass
    return None

def get_main_activity(item):
    """Extract main activity description from EMTAK data."""
    activities = item.get('yldandmed', {}).get('teatatud_tegevusalad', [])
    if not activities:
        return ""
    for a in activities:
        if a.get('on_pohitegevusala'):
            code = a.get('emtak_kood', '')
            desc = a.get('emtak_tekstina', '')
            return f"{code} {desc}".strip()
    # fallback: first activity
    a = activities[0]
    return f"{a.get('emtak_kood', '')} {a.get('emtak_tekstina', '')}".strip()

def filter_by_employees(results, min_employees=None, max_employees=None):
    """Post-filter search results by employee count."""
    for item in results:
        emp = get_latest_employees(item)
        if min_employees is not None and (emp is None or emp < min_employees):
            continue
        if max_employees is not None and (emp is not None and emp > max_employees):
            continue
        yield item

def filter_growing(results):
    """Post-filter: only companies where latest employee_count > previous year's."""
    for item in results:
        reports = item.get('yldandmed', {}).get('info_majandusaasta_aruannetest', [])
        if len(reports) < 2:
            continue
        sorted_reports = sorted(reports, key=lambda r: r.get('majandusaasta_perioodi_lopp_kpv', ''), reverse=True)
        latest = prev = None
        for r in sorted_reports:
            emp = r.get('tootajate_arv')
            if emp is not None:
                try:
                    val = int(emp)
                except (TypeError, ValueError):
                    continue
                if latest is None:
                    latest = val
                elif prev is None:
                    prev = val
                    break
        if latest is not None and prev is not None and latest > prev:
            yield item

def shorten_status(status, to_en=False):
    """Shorten status labels for compact display."""
    status_map = {
        "Registrisse kantud": "Active", "Entered into register": "Active",
        "Kustutatud": "Deleted", "Deleted": "Deleted",
        "Likvideerimisel": "Liquidating", "In liquidation": "Liquidating",
        "Pankrotis": "Bankrupt", "Bankrupt": "Bankrupt",
        "Registrist kustutatud": "Deleted", "Deleted from register": "Deleted",
    }
    if not status:
        return "Unknown"
    for key, short in status_map.items():
        if key in status:
            return short
    return status[:20]

def display_company_summary(items, lang="et"):
    """Display companies as a compact summary table (one row per company)."""
    lbl = UI_LABELS[lang]; to_en = (lang == "en")
    t = Table(title="Companies" if to_en else "Ettevotted", box=box.ROUNDED, header_style="bold yellow", expand=True)
    t.add_column("Name" if to_en else "Nimi", style="bold white", max_width=35)
    t.add_column("Code" if to_en else "Kood", style="cyan", justify="right")
    t.add_column("County" if to_en else "Maakond", style="dim")
    t.add_column("Industry" if to_en else "Tegevusala", max_width=30)
    t.add_column("Capital" if to_en else "Kapital", justify="right", style="yellow")
    t.add_column("Emp" if to_en else "Toot", justify="right", style="green")
    t.add_column("Founded" if to_en else "Asutatud", style="dim")
    t.add_column("Status" if to_en else "Staatus")
    count = 0
    for item in items:
        count += 1
        name = item.get('nimi', 'N/A')
        code = str(item.get('ariregistri_kood', ''))
        county = item.get('asukoha_ehak_tekstina', '')
        if county:
            parts = [p.strip() for p in county.split(',')]
            county = next((p for p in reversed(parts) if 'maakond' in p), parts[-1] if parts else '')
        activity = get_main_activity(item)
        if len(activity) > 30:
            activity = activity[:27] + "..."
        # Capital from yldandmed
        cap_amt, cap_cur = SQLiteBackend._extract_latest_capital(item)
        if cap_amt is not None:
            cap_str = f"{cap_amt:,.0f}" if cap_amt == int(cap_amt) else f"{cap_amt:,.2f}"
        else:
            cap_str = "-"
        emp = get_latest_employees(item)
        emp_str = str(emp) if emp is not None else "-"
        founded = item.get('yldandmed', {}).get('esmaregistreerimise_kpv', '') or ''
        if founded and len(founded) >= 10:
            founded = founded[:10]
        status = item.get('yldandmed', {}).get('staatus_tekstina', '') or item.get('ettevotja_staatus_tekstina', '') or ''
        status = shorten_status(status, to_en)
        t.add_row(name, code, county, activity, cap_str, emp_str, founded, status)
    console.print(t)
    console.print(f"\n[success]{'Found' if to_en else 'Leitud'}: {count} {'companies' if to_en else 'ettevottet'}[/success]")
    return count

def display_industry_list(lang="et"):
    """Display all available industry names grouped by category."""
    to_en = (lang == "en")
    categories = {
        "IT & Technology": ["software", "it", "tech", "programming", "consulting", "it consulting", "data processing", "hosting", "web", "telecom"],
        "Manufacturing": ["manufacturing", "food manufacturing", "beverages", "textiles", "clothing", "wood", "paper", "printing", "chemicals", "pharmaceuticals", "plastics", "metals", "electronics", "machinery", "automotive", "furniture"],
        "Construction": ["construction", "building", "civil engineering", "renovation", "plumbing", "electrical"],
        "Trade & Retail": ["retail", "wholesale", "trade", "e-commerce", "car sales", "grocery"],
        "Food & Hospitality": ["restaurant", "food", "catering", "hotel", "accommodation", "hospitality", "bar", "cafe"],
        "Transport & Logistics": ["transport", "logistics", "warehousing", "trucking", "freight", "taxi", "courier", "shipping", "aviation"],
        "Real Estate": ["real estate", "property", "rental"],
        "Finance & Insurance": ["finance", "banking", "insurance", "investment", "fintech"],
        "Professional Services": ["legal", "law", "accounting", "audit", "management consulting", "architecture", "engineering", "design", "advertising", "marketing", "research", "translation"],
        "Healthcare": ["healthcare", "medical", "dental", "pharmacy", "veterinary"],
        "Education": ["education", "training", "school"],
        "Agriculture": ["agriculture", "farming", "forestry", "fishing"],
        "Energy & Mining": ["energy", "electricity", "mining", "oil", "gas"],
        "Media & Entertainment": ["media", "publishing", "film", "tv", "gaming", "music"],
        "Other Services": ["cleaning", "security", "staffing", "recruitment", "beauty", "hairdressing", "fitness", "sports", "waste", "recycling", "ngo", "nonprofit"],
    }
    title = "Available Industries" if to_en else "Saadaolevad tegevusalad"
    tree = Tree(f"[bold blue]{title}[/bold blue]")
    for cat, names in categories.items():
        node = tree.add(f"[bold yellow]{cat}[/bold yellow]")
        for name in names:
            codes = INDUSTRY_MAP.get(name, [])
            node.add(f"[cyan]{name}[/cyan] -> EMTAK {', '.join(codes)}")
    console.print(tree)

def export_csv(db, output_path, lang="et", emtak=None, location=None, status=None,
               legal_form=None, founded_after=None, founded_before=None,
               min_employees=None, max_employees=None, limit=None,
               min_capital=None, max_capital=None, has_email=False, has_phone=False, has_website=False):
    """Export filtered companies to CSV with flattened columns."""
    to_en = (lang == "en")
    results = db.search(emtak=emtak, location=location, status=status,
                        legal_form=legal_form, founded_after=founded_after,
                        founded_before=founded_before, limit=limit,
                        min_capital=min_capital, max_capital=max_capital,
                        has_email=has_email, has_phone=has_phone, has_website=has_website)
    if min_employees or max_employees:
        results = filter_by_employees(results, min_employees, max_employees)

    headers = ["code", "name", "status", "county", "city", "legal_form", "founded",
               "main_industry_code", "main_industry_name", "employees", "capital", "capital_currency",
               "vat_number", "email", "phone", "website"]

    count = 0
    with open(output_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for item in results:
            count += 1
            yld = item.get('yldandmed', {})
            # Extract contacts
            contacts = yld.get('sidevahendid', [])
            email = phone = website = ""
            for c in contacts:
                ctype = c.get('liik_tekstina', '')
                val = c.get('sisu', '')
                if 'mail' in ctype.lower() or 'post' in ctype.lower():
                    email = email or val
                elif 'telefon' in ctype.lower() or 'mobiil' in ctype.lower():
                    phone = phone or val
                elif 'www' in ctype.lower() or 'internet' in ctype.lower():
                    website = website or val
            # Extract main activity
            activities = yld.get('teatatud_tegevusalad', [])
            main_code = main_name = ""
            for a in activities:
                if a.get('on_pohitegevusala'):
                    main_code = a.get('emtak_kood', '')
                    main_name = a.get('emtak_tekstina', '')
                    break
            if not main_code and activities:
                main_code = activities[0].get('emtak_kood', '')
                main_name = activities[0].get('emtak_tekstina', '')
            # County/city from ehak
            ehak = item.get('asukoha_ehak_tekstina', '')
            county = city = ""
            if ehak:
                parts = [p.strip() for p in ehak.split(',')]
                county = next((p for p in reversed(parts) if 'maakond' in p), '')
                city = next((p for p in parts if 'linn' in p or 'vald' in p), '')

            emp = get_latest_employees(item)
            status_val = yld.get('staatus_tekstina', '') or item.get('ettevotja_staatus_tekstina', '')
            if to_en:
                status_val = translate_value(status_val, True)
                main_name = translate_value(main_name, True)

            cap_amt, cap_cur = SQLiteBackend._extract_latest_capital(item)
            writer.writerow([
                item.get('ariregistri_kood', ''), item.get('nimi', ''), status_val,
                county, city, item.get('ettevotja_oiguslik_vorm', ''),
                yld.get('esmaregistreerimise_kpv', '') or item.get('ettevotja_esmakande_kpv', ''),
                main_code, main_name, emp if emp is not None else '',
                cap_amt if cap_amt is not None else '', cap_cur or '',
                item.get('kmkr_nr', ''),
                email, phone, website
            ])
    console.print(f"[success]Exported {count} companies to {output_path}[/success]")

def cmd_report(db, report_type, lang="et", **kwargs):
    """Execute a pre-built business report."""
    to_en = (lang == "en")

    if report_type == "market-overview":
        console.print(f"\n[bold blue]{'Market Overview' if to_en else 'Turu ulevaade'}[/bold blue]\n")
        display_stats(db.get_stats(), lang=lang)
        console.print()
        display_analysis(db.analyze(by="county", top=10), by="county", lang=lang)
        console.print()
        display_analysis(db.analyze(by="emtak", top=15), by="emtak", lang=lang)
        console.print()
        display_analysis(db.analyze(by="year", top=30), by="year", lang=lang)
        console.print()
        display_analysis(db.analyze(by="legal-form", top=10), by="legal-form", lang=lang)

    elif report_type == "new-companies":
        period = kwargs.get('period', '2024')
        title = f"{'New Companies' if to_en else 'Uued ettevotted'} {period}"
        console.print(f"\n[bold blue]{title}[/bold blue]\n")
        fa = f"{period}-01-01"; fb = f"{period}-12-31"
        total = db.analyze(by="status", founded_after=fa, founded_before=fb, top=100)
        total_count = sum(c for _, c in total)
        console.print(f"[success]{'Total new companies' if to_en else 'Uusi ettevotteid kokku'}: {total_count:,}[/success]\n")
        display_analysis(db.analyze(by="emtak", founded_after=fa, founded_before=fb, top=15), by="emtak", lang=lang)
        console.print()
        display_analysis(db.analyze(by="county", founded_after=fa, founded_before=fb, top=10), by="county", lang=lang)
        console.print()
        display_analysis(db.analyze(by="legal-form", founded_after=fa, founded_before=fb, top=10), by="legal-form", lang=lang)

    elif report_type == "top-industries":
        location = kwargs.get('location')
        title = f"{'Top Industries' if to_en else 'Suurimad tegevusalad'}"
        if location:
            title += f" - {location}"
        console.print(f"\n[bold blue]{title}[/bold blue]\n")
        display_analysis(db.analyze(by="emtak", location=location, top=20), by="emtak", lang=lang)

    elif report_type == "industry-growth":
        industry = kwargs.get('industry')
        emtak = resolve_industry(industry) if industry else None
        if industry and not emtak:
            return
        title = f"{'Industry Growth' if to_en else 'Tegevusala kasv'}: {industry or 'All'}"
        console.print(f"\n[bold blue]{title}[/bold blue]\n")
        display_analysis(db.analyze(by="year", emtak=emtak, top=50), by="year", lang=lang)
        console.print()
        display_analysis(db.analyze(by="county", emtak=emtak, top=10), by="county", lang=lang)

    elif report_type == "regional":
        county = kwargs.get('county')
        if not county:
            console.print("[warning]Please specify --county[/warning]"); return
        title = f"{'Regional Report' if to_en else 'Piirkondlik aruanne'}: {county}"
        console.print(f"\n[bold blue]{title}[/bold blue]\n")
        display_analysis(db.analyze(by="status", location=county, top=10), by="status", lang=lang)
        console.print()
        display_analysis(db.analyze(by="emtak", location=county, top=15), by="emtak", lang=lang)
        console.print()
        display_analysis(db.analyze(by="year", location=county, top=30), by="year", lang=lang)
        console.print()
        display_analysis(db.analyze(by="legal-form", location=county, top=10), by="legal-form", lang=lang)

    elif report_type == "bankruptcies":
        period = kwargs.get('period')
        title = f"{'Bankruptcies & Liquidations' if to_en else 'Pankrotid ja likvideerimised'}"
        if period:
            title += f" {period}"
        console.print(f"\n[bold blue]{title}[/bold blue]\n")
        fa = f"{period}-01-01" if period else None
        fb = f"{period}-12-31" if period else None
        for st in ["Likvideerimisel", "Pankrotis"]:
            results = db.analyze(by="emtak", status=st, founded_after=fa, founded_before=fb, top=10)
            if results:
                st_label = translate_value(st, to_en)
                console.print(f"\n[bold yellow]{st_label}[/bold yellow]")
                display_analysis(results, by="emtak", lang=lang)
        console.print()
        for st in ["Likvideerimisel", "Pankrotis"]:
            results = db.analyze(by="county", status=st, founded_after=fa, founded_before=fb, top=10)
            if results:
                st_label = translate_value(st, to_en)
                console.print(f"\n[bold yellow]{st_label} - {'by county' if to_en else 'maakonniti'}[/bold yellow]")
                display_analysis(results, by="county", lang=lang)
    elif report_type == "employee-trend":
        code = kwargs.get('code')
        industry = kwargs.get('industry')
        emtak = resolve_industry(industry) if industry else None
        if industry and not emtak:
            return
        if code:
            title = f"{'Employee Trend' if to_en else 'Tootajate trend'}: {code}"
            console.print(f"\n[bold blue]{title}[/bold blue]\n")
            trend = db.employee_trend(code=code)
            display_employee_trend(trend, code=code, lang=lang)
        else:
            title = f"{'Employee Trend' if to_en else 'Tootajate trend'}: {industry or 'All'}"
            console.print(f"\n[bold blue]{title}[/bold blue]\n")
            location = kwargs.get('location')
            trend = db.employee_trend(emtak=emtak, location=location)
            display_employee_trend(trend, lang=lang)
    else:
        console.print(f"[warning]Unknown report type: {report_type}[/warning]")
        console.print("Available: market-overview, new-companies, top-industries, industry-growth, regional, bankruptcies, employee-trend")

def display_person_results(results, lang="et"):
    to_en = (lang == "en")
    t = Table(title="Person Search Results" if to_en else "Isikuotsingu tulemused", box=box.ROUNDED, header_style="bold yellow", expand=True)
    t.add_column("Person" if to_en else "Isik", style="bold white", max_width=30)
    t.add_column("ID Code" if to_en else "Isikukood", style="magenta")
    t.add_column("Company" if to_en else "Ettevote", max_width=30)
    t.add_column("Code" if to_en else "Kood", style="cyan", justify="right")
    t.add_column("Source" if to_en else "Allikas", style="dim")
    t.add_column("Role" if to_en else "Roll", max_width=25)
    t.add_column("Since" if to_en else "Alates", style="dim")
    for r in results:
        id_code = r.get('id_code') or r.get('id_hash', '')[:8] + '...' if r.get('id_hash') else '-'
        t.add_row(
            r.get('full_name', ''), id_code,
            r.get('company_name', ''), str(r.get('company_code', '')),
            r.get('source', ''), translate_value(r.get('role', ''), to_en),
            r.get('start_date', '') or '-')
    console.print(t)
    console.print(f"\n[success]{'Found' if to_en else 'Leitud'}: {len(results)} {'records' if to_en else 'kirjet'}[/success]")

def display_person_network(results, name=None, lang="et"):
    to_en = (lang == "en")
    if not results:
        console.print(f"[warning]{'No results found.' if to_en else 'Tulemusi ei leitud.'}[/warning]")
        return
    title = f"{'Network' if to_en else 'Voorgustik'}: {name or results[0].get('full_name', 'Unknown')}"
    tree = Tree(f"[bold blue]{title}[/bold blue]")
    # Group by source
    by_source = defaultdict(list)
    for r in results:
        by_source[r['source']].append(r)
    source_labels = {'board': 'Board Member' if to_en else 'Juhatuse liige',
                     'shareholder': 'Shareholder' if to_en else 'Osanik',
                     'beneficiary': 'Beneficiary' if to_en else 'Kasusaaja'}
    for src, items in by_source.items():
        node = tree.add(f"[bold yellow]{source_labels.get(src, src)}[/bold yellow] ({len(items)})")
        for r in items:
            status = r.get('company_status', '')
            status_tag = f" [dim]({shorten_status(status, to_en)})[/dim]" if status else ""
            detail = ""
            if r.get('ownership_pct'):
                detail = f" [green]{r['ownership_pct']:.1f}%[/green]"
            elif r.get('role'):
                detail = f" [dim]{translate_value(r['role'], to_en)}[/dim]"
            node.add(f"[cyan]{r.get('company_name', 'N/A')}[/cyan] ({r.get('company_code', '')}){detail}{status_tag}")
    console.print(tree)

def display_group_tree(group_data, lang="et"):
    to_en = (lang == "en")
    company = group_data.get("company", {})
    if not company:
        console.print(f"[warning]{'Company not found.' if to_en else 'Ettevotet ei leitud.'}[/warning]")
        return
    title = f"{'Corporate Group' if to_en else 'Kontsern'}: {company.get('name', 'N/A')} ({company.get('code', '')})"
    tree = Tree(f"[bold blue]{title}[/bold blue]")
    parents = group_data.get("parents", [])
    if parents:
        pnode = tree.add(f"[bold yellow]{'Shareholders (owners)' if to_en else 'Osanikud (omanikud)'}[/bold yellow]")
        for p in parents:
            pct = f" [green]{p['ownership_pct']:.1f}%[/green]" if p.get('ownership_pct') else ""
            amt = f" ({p.get('contribution_amount', '')}{' ' + p.get('currency', '') if p.get('currency') else ''})" if p.get('contribution_amount') else ""
            pnode.add(f"{p.get('full_name', 'N/A')} [dim]({p.get('id_code', '-')})[/dim]{pct}{amt}")
    subs = group_data.get("subsidiaries", [])
    if subs:
        snode = tree.add(f"[bold yellow]{'Subsidiaries' if to_en else 'Tutarettevotted'}[/bold yellow]")
        # Build tree by depth
        depth_nodes = {0: snode}
        for s in sorted(subs, key=lambda x: x.get('depth', 1)):
            depth = s.get('depth', 1)
            parent = depth_nodes.get(depth - 1, snode)
            pct = f" [green]{s['ownership_pct']:.1f}%[/green]" if s.get('ownership_pct') else ""
            n = parent.add(f"[cyan]{s.get('company_name', 'N/A')}[/cyan] ({s.get('company_code', '')}){pct}")
            depth_nodes[depth] = n
    console.print(tree)

def display_employee_trend(trend, code=None, lang="et"):
    to_en = (lang == "en")
    title = "Employee Trend" if to_en else "Tootajate trend"
    if code:
        title += f" - {code}"
    t = Table(title=title, box=box.ROUNDED, header_style="bold yellow", expand=True)
    t.add_column("Year" if to_en else "Aasta", style="cyan")
    t.add_column("Employees" if to_en else "Tootajad", justify="right", style="bold white")
    t.add_column("Change" if to_en else "Muutus", justify="right")
    t.add_column("% Change" if to_en else "% muutus", justify="right")
    if not trend:
        console.print(f"[warning]{'No trend data available.' if to_en else 'Trendiandmed puuduvad.'}[/warning]")
        return
    # For industry-wide, also show company count
    has_companies = 'companies' in trend[0]
    if has_companies:
        t.add_column("Companies" if to_en else "Ettevotteid", justify="right", style="dim")
    prev = None
    for entry in trend:
        emp = entry['employees']
        change = ""
        pct_change = ""
        if prev is not None:
            diff = emp - prev
            change = f"+{diff}" if diff > 0 else str(diff)
            if prev > 0:
                pct = (diff / prev) * 100
                color = "green" if pct > 0 else "red" if pct < 0 else ""
                pct_change = f"[{color}]{pct:+.1f}%[/{color}]" if color else f"{pct:+.1f}%"
                change = f"[{color}]{change}[/{color}]" if color else change
        row = [entry['year'], f"{emp:,}", change, pct_change]
        if has_companies:
            row.append(f"{entry.get('companies', 0):,}")
        t.add_row(*row)
        prev = emp
    console.print(t)

def display_analysis(results, by, lang="et"):
    lbl = UI_LABELS[lang]; to_en = (lang == "en")
    by_label = lbl["analysis_by"].get(by, by)
    title = f"{lbl['analysis_title']}: {by_label}"
    if not results:
        console.print(f"[warning]{lbl['no_results']}[/warning]"); return
    total = sum(cnt for _, cnt in results)
    t = Table(title=title, box=box.ROUNDED, header_style="bold yellow", expand=True)
    t.add_column(lbl["rank"], justify="right", style="dim", width=5)
    t.add_column(lbl["group"], style="cyan")
    t.add_column(lbl["count"], justify="right", style="bold white")
    t.add_column(lbl["pct"], justify="left")
    for i, (grp, cnt) in enumerate(results, 1):
        pct = (cnt / total * 100) if total else 0
        bar_len = int(pct / 2)
        bar = f"{'#' * bar_len}{'.' * (50 - bar_len)} {pct:.1f}%"
        display_grp = translate_value(grp, to_en) if grp else "N/A"
        t.add_row(str(i), str(display_grp), f"{cnt:,}", bar)
    console.print(t)
    console.print(f"\n[success]Total: {total:,}[/success]")

# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Estonian Registry CLI - Business Intelligence for Estonian Companies")
    parser.add_argument("--no-db", action="store_true")
    parser.add_argument("--en", action="store_true"); parser.add_argument("--ee", action="store_true"); parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--list-industries", action="store_true", help="Show all available industry names")
    sub = parser.add_subparsers(dest="cmd")

    # Core commands
    for n, al in {"sync": ["sünk"], "merge": ["ühenda"]}.items():
        sp = sub.add_parser(n, aliases=al); sp.add_argument("--force", action="store_true")
    sub.add_parser("enrich", aliases=["rikasta"]).add_argument("codes", nargs="+")
    sub.add_parser("stats", aliases=["statistika"])

    # Search command (detailed dossier view)
    srch = sub.add_parser("search", aliases=["otsi"])
    srch.add_argument("term", nargs="?"); srch.add_argument("-l", "--location"); srch.add_argument("-s", "--status"); srch.add_argument("-p", "--person")
    srch.add_argument("--emtak"); srch.add_argument("--industry"); srch.add_argument("--founded-after"); srch.add_argument("--founded-before"); srch.add_argument("--legal-form")
    srch.add_argument("--json", action="store_true"); srch.add_argument("-t", "--translate", action="store_true"); srch.add_argument("--limit", type=int, default=5)
    for s in ["core", "general", "history", "personnel", "ownership", "beneficiaries", "operations", "registry", "enrichment"]:
        srch.add_argument(f"--{s}", action="append_const", dest="sections", const=s)

    # Find command (compact business-user search)
    fnd = sub.add_parser("find", aliases=["leia"], help="Find companies with simple filters")
    fnd.add_argument("query", nargs="?", help="Company name or code")
    fnd.add_argument("--industry", help="Industry name (e.g., software, construction, restaurant)")
    fnd.add_argument("-l", "--location", help="County or city name")
    fnd.add_argument("-s", "--status", help="Company status filter")
    fnd.add_argument("--min-employees", type=int, help="Minimum employee count")
    fnd.add_argument("--max-employees", type=int, help="Maximum employee count")
    fnd.add_argument("--founded-after", help="Founded after date (YYYY-MM-DD)")
    fnd.add_argument("--founded-before", help="Founded before date (YYYY-MM-DD)")
    fnd.add_argument("--legal-form", help="Legal form filter")
    fnd.add_argument("--min-capital", type=float, help="Minimum share capital")
    fnd.add_argument("--max-capital", type=float, help="Maximum share capital")
    fnd.add_argument("--has-email", action="store_true", help="Only companies with email")
    fnd.add_argument("--has-phone", action="store_true", help="Only companies with phone")
    fnd.add_argument("--has-website", action="store_true", help="Only companies with website")
    fnd.add_argument("--growing", action="store_true", help="Only companies with growing employee count")
    fnd.add_argument("--limit", type=int, default=50, help="Max results (default: 50)")
    fnd.add_argument("--full", action="store_true", help="Show full dossier instead of summary")
    fnd.add_argument("--json", action="store_true", help="Output as JSON")
    fnd.add_argument("--csv", help="Export results to CSV file")

    # Analyze command
    anl = sub.add_parser("analyze", aliases=["analüüs"])
    anl.add_argument("--by", required=True, choices=["county", "status", "legal-form", "emtak", "year", "capital-range", "employee-range", "role", "country"])
    anl.add_argument("--emtak"); anl.add_argument("--industry"); anl.add_argument("--location"); anl.add_argument("--status"); anl.add_argument("--legal-form")
    anl.add_argument("--founded-after"); anl.add_argument("--founded-before")
    anl.add_argument("--top", type=int, default=20); anl.add_argument("--json", action="store_true")

    # Person command
    per = sub.add_parser("person", aliases=["isik"], help="Search persons across all companies")
    per.add_argument("name", nargs="?", help="Person name to search")
    per.add_argument("--id", dest="id_code", help="Person or company ID code (exact)")
    per.add_argument("--role", help="Filter by role (LIKE match)")
    per.add_argument("--source", choices=["board", "shareholder", "beneficiary"], help="Filter by source")
    per.add_argument("--code", help="Filter by company code")
    per.add_argument("--network", action="store_true", help="Show all companies for this person")
    per.add_argument("--limit", type=int, default=50)

    # Group command
    grp = sub.add_parser("group", aliases=["kontsern"], help="Corporate ownership chain mapping")
    grp.add_argument("code", help="Company registry code")
    grp.add_argument("--direction", choices=["up", "down", "both"], default="both", help="Direction: up (owners), down (subsidiaries), both")
    grp.add_argument("--depth", type=int, default=5, help="Max recursion depth")

    # Report command (pre-built business reports)
    rpt = sub.add_parser("report", aliases=["aruanne"], help="Pre-built business intelligence reports")
    rpt.add_argument("type", choices=["market-overview", "new-companies", "top-industries", "industry-growth", "regional", "bankruptcies", "employee-trend"])
    rpt.add_argument("--period", help="Year for time-based reports (e.g., 2024)")
    rpt.add_argument("--industry", help="Industry name for industry reports")
    rpt.add_argument("-l", "--location", help="Location filter")
    rpt.add_argument("--county", help="County for regional report")
    rpt.add_argument("--code", help="Company code for company-specific reports")

    # Export command (improved with filters)
    exp = sub.add_parser("export", aliases=["ekspordi"], help="Export companies to CSV or JSON")
    exp.add_argument("output", help="Output file (.csv or .json)")
    exp.add_argument("--industry", help="Industry name filter")
    exp.add_argument("-l", "--location", help="Location filter")
    exp.add_argument("-s", "--status", help="Status filter")
    exp.add_argument("--legal-form", help="Legal form filter")
    exp.add_argument("--founded-after"); exp.add_argument("--founded-before")
    exp.add_argument("--min-employees", type=int); exp.add_argument("--max-employees", type=int)
    exp.add_argument("--min-capital", type=float); exp.add_argument("--max-capital", type=float)
    exp.add_argument("--has-email", action="store_true"); exp.add_argument("--has-phone", action="store_true"); exp.add_argument("--has-website", action="store_true")
    exp.add_argument("--limit", type=int, help="Max companies to export")

    args = parser.parse_args(); setup_logging(args.verbose)

    # Language detection
    et_cmds = ["otsi", "rikasta", "ühenda", "sünk", "ekspordi", "analüüs", "statistika", "leia", "aruanne", "isik", "kontsern"]
    en_cmds = ["search", "enrich", "merge", "sync", "export", "analyze", "stats", "find", "report", "person", "group"]
    cmd_typed = sys.argv[1] if len(sys.argv) > 1 else ""
    if args.en: lang = "en"
    elif args.ee: lang = "et"
    else: lang = "et" if (cmd_typed in et_cmds or (cmd_typed not in en_cmds and cmd_typed != "")) else "en"

    # --list-industries (no DB needed)
    if args.list_industries:
        display_industry_list(lang=lang); return

    reg = EstonianRegistry(use_db=not args.no_db)

    if args.cmd in ["stats", "statistika"]:
        display_stats(reg.db.get_stats(), lang=lang)

    elif args.cmd in ["sync", "sünk"]: reg.sync(force=getattr(args, 'force', False))
    elif args.cmd in ["merge", "ühenda"]: reg.merge(force=getattr(args, 'force', False))
    elif args.cmd in ["enrich", "rikasta"]: reg.enrich(args.codes)

    elif args.cmd in ["search", "otsi"]:
        emtak = args.emtak
        if args.industry:
            emtak = resolve_industry(args.industry)
            if not emtak: return
        results = reg.db.search(term=args.term, person=args.person, location=args.location, status=args.status,
                                limit=args.limit, emtak=emtak, founded_after=args.founded_after,
                                founded_before=args.founded_before, legal_form=args.legal_form)
        sections, count = args.sections or ["all"], 0
        for item in results:
            count += 1
            if args.json: console.print(Syntax(json.dumps(translate_item(item, to_en=(args.translate or lang=="en")), indent=2, ensure_ascii=False), "json", theme="monokai"))
            else: display_company(item, sections=sections, lang=lang)
        if count == 0: console.print(f"[warning]{UI_LABELS[lang]['no_results']}[/warning]")
        else: console.print(f"\n[success]{UI_LABELS[lang]['results_found']}: {count}[/success]")

    elif args.cmd in ["find", "leia"]:
        emtak = None
        if args.industry:
            emtak = resolve_industry(args.industry)
            if not emtak: return
        # Use a higher limit for post-filtering by employees
        fetch_limit = args.limit
        if args.min_employees or args.max_employees or args.growing:
            fetch_limit = None  # Fetch all, filter in Python
        results = reg.db.search(term=args.query, location=args.location, status=args.status,
                                limit=fetch_limit, emtak=emtak, founded_after=args.founded_after,
                                founded_before=args.founded_before, legal_form=args.legal_form,
                                min_capital=args.min_capital, max_capital=args.max_capital,
                                has_email=args.has_email, has_phone=args.has_phone, has_website=args.has_website)
        if args.min_employees or args.max_employees:
            results = filter_by_employees(results, args.min_employees, args.max_employees)
        if args.growing:
            results = filter_growing(results)
        # Apply limit after employee filter
        def limited(gen, n):
            for i, item in enumerate(gen):
                if n and i >= n: break
                yield item
        results = limited(results, args.limit)
        if args.csv:
            # Collect results and export
            items = list(results)
            export_csv(reg.db, args.csv, lang=lang, emtak=emtak, location=args.location,
                       status=args.status, legal_form=args.legal_form,
                       founded_after=args.founded_after, founded_before=args.founded_before,
                       min_employees=args.min_employees, max_employees=args.max_employees, limit=args.limit)
        elif args.json:
            items = list(results)
            console.print(Syntax(json.dumps([translate_item(i, to_en=(lang=="en")) for i in items], indent=2, ensure_ascii=False), "json", theme="monokai"))
        elif args.full:
            count = 0
            for item in results:
                count += 1; display_company(item, lang=lang)
            if count == 0: console.print(f"[warning]{UI_LABELS[lang]['no_results']}[/warning]")
        else:
            display_company_summary(results, lang=lang)

    elif args.cmd in ["analyze", "analüüs"]:
        emtak = args.emtak
        if args.industry:
            emtak = resolve_industry(args.industry)
            if not emtak: return
        results = reg.db.analyze(by=args.by, emtak=emtak, location=args.location, status=args.status,
                                 legal_form=args.legal_form, founded_after=args.founded_after,
                                 founded_before=args.founded_before, top=args.top)
        if args.json:
            console.print(Syntax(json.dumps([{"group": g, "count": c} for g, c in results], indent=2, ensure_ascii=False), "json", theme="monokai"))
        else:
            display_analysis(results, by=args.by, lang=lang)

    elif args.cmd in ["person", "isik"]:
        if args.network:
            results = reg.db.person_network(name=args.name, id_code=args.id_code)
            display_person_network(results, name=args.name or args.id_code, lang=lang)
        else:
            results = reg.db.search_persons(name=args.name, id_code=args.id_code, role=args.role,
                                            source=args.source, company_code=args.code, limit=args.limit)
            display_person_results(results, lang=lang)

    elif args.cmd in ["group", "kontsern"]:
        group_data = reg.db.find_group(args.code, direction=args.direction, max_depth=args.depth)
        display_group_tree(group_data, lang=lang)

    elif args.cmd in ["report", "aruanne"]:
        cmd_report(reg.db, args.type, lang=lang, period=args.period,
                   industry=args.industry, location=args.location, county=args.county,
                   code=getattr(args, 'code', None))

    elif args.cmd in ["export", "ekspordi"]:
        output = Path(args.output)
        emtak = None
        if args.industry:
            emtak = resolve_industry(args.industry)
            if not emtak: return
        if output.suffix.lower() == '.csv':
            export_csv(reg.db, output, lang=lang, emtak=emtak, location=args.location,
                       status=args.status, legal_form=args.legal_form,
                       founded_after=args.founded_after, founded_before=args.founded_before,
                       min_employees=args.min_employees, max_employees=args.max_employees, limit=args.limit,
                       min_capital=args.min_capital, max_capital=args.max_capital,
                       has_email=args.has_email, has_phone=args.has_phone, has_website=args.has_website)
        else:
            # JSON export (original behavior with filters)
            results = reg.db.search(emtak=emtak, location=args.location, status=args.status,
                                    legal_form=args.legal_form, founded_after=args.founded_after,
                                    founded_before=args.founded_before, limit=args.limit,
                                    min_capital=args.min_capital, max_capital=args.max_capital,
                                    has_email=args.has_email, has_phone=args.has_phone, has_website=args.has_website)
            if args.min_employees or args.max_employees:
                results = filter_by_employees(results, args.min_employees, args.max_employees)
            data = [translate_item(i, to_en=(lang=="en")) for i in results]
            with open(output, 'w', encoding='utf-8') as f: json.dump(data, f, ensure_ascii=False, indent=2)
            console.print(f"[success]Exported {len(data)} companies to {output}[/success]")

if __name__ == "__main__": main()
