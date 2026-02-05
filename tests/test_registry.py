import pytest
import json
import sqlite3
from pathlib import Path
from registry import EstonianRegistry, RegistryDB, translate_item, UI_LABELS

def test_translation_logic():
    item = {
        "ariregistri_kood": 12345,
        "nimi": "Test Company",
        "staatus": "Registrisse kantud",
        "osanikud": [
            {"isiku_roll_tekstina": "Juhatuse liige"}
        ]
    }
    # Translate to English
    translated = translate_item(item, to_en=True)
    
    assert translated["registry_code"] == 12345
    assert translated["name"] == "Test Company"
    assert "Entered into register" in translated["status"]
    assert "Management board member" in translated["shareholders"][0]["role_description"]

def test_registry_init(tmp_path):
    data_dir = tmp_path / "data"
    reg = EstonianRegistry(data_dir=str(data_dir), use_db=True)
    
    assert (data_dir / "downloads").exists()
    assert (data_dir / "extracted").exists()
    assert reg.db is not None
    assert reg.db_path.exists()

def test_db_operations(tmp_path):
    db_path = tmp_path / "test.db"
    db = RegistryDB(db_path)
    
    # Base data
    batch = [{
        "ariregistri_kood": 10001,
        "nimi": "Alpha LLC",
        "staatus": "Registrisse kantud",
        "aadress_maakond": "Harju",
        "aadress_linn": "Tallinn",
        "ettevotja_oiguslik_vorm": "Osaühing",
        "esmakande_kuupaev": "2020-01-01"
    }]
    db.insert_batch_base(batch)
    
    # Update with JSON
    db.update_batch_json("osanikud", {10001: [{"nimi_arinimi": "John Doe"}]})
    
    results = list(db.search(term="Alpha"))
    assert len(results) == 1
    assert results[0]["nimi"] == "Alpha LLC"
    assert "osanikud" in results[0]
    assert results[0]["osanikud"][0]["nimi_arinimi"] == "John Doe"
    
    stats = db.get_stats()
    assert stats["total"] == 1

def test_ui_labels():
    assert "Toimik" in UI_LABELS["et"]["dossier"]
    assert "Dossier" in UI_LABELS["en"]["dossier"]

def test_enrichment_storage(tmp_path):
    db_path = tmp_path / "test_enrich.db"
    db = RegistryDB(db_path)
    
    db.insert_batch_base([{"ariregistri_kood": 123, "nimi": "Test"}])
    enrich_data = {"processed_at": "now", "unmasked_ids": {"Test Person": "12345678901"}}
    db.update_enrichment(123, enrich_data)
    
    row = list(db.search(term="123"))[0]
    assert "enrichment" in row
    assert row["enrichment"]["unmasked_ids"]["Test Person"] == "12345678901"