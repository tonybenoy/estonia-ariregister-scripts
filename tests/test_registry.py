import pytest
from pathlib import Path
from registry import EstonianRegistry, translate_item

def test_translation_logic():
    item = {
        "ariregistri_kood": 12345,
        "nimi": "Test Company",
        "staatus": "Registrisse kantud",
        "osanikud": [
            {"isikukood_registrikood": "123", "isiku_roll": "Juhatuse liige"}
        ]
    }
    translated = translate_item(item)
    
    assert translated["registry_code"] == 12345
    assert translated["name"] == "Test Company"
    assert translated["status"] == "Entered into register"
    assert translated["shareholders"][0]["role"] == "Management board member"

def test_registry_init(tmp_path):
    # Test initialization with a temporary directory
    data_dir = tmp_path / "data"
    reg = EstonianRegistry(data_dir=str(data_dir))
    
    assert (data_dir / "downloads").exists()
    assert (data_dir / "chunks").exists()
    assert reg.db is None  # Should be None since DB doesn't exist yet

def test_db_operations(tmp_path):
    from registry import RegistryDB
    db_path = tmp_path / "test.db"
    db = RegistryDB(db_path)
    
    batch = [{
        "ariregistri_kood": 10001,
        "nimi": "Alpha LLC",
        "staatus": "Active",
        "aadress_maakond": "Harju",
        "aadress_linn": "Tallinn",
        "ettevotja_oiguslik_vorm": "OÜ",
        "esmakande_kuupaev": "2020-01-01"
    }]
    db.insert_batch(batch)
    
    results = list(db.search(term="Alpha"))
    assert len(results) == 1
    assert results[0]["nimi"] == "Alpha LLC"
    
    stats = db.get_stats()
    assert stats["total"] == 1
