import pytest

from pathlib import Path
import json
import os
import sys
import importlib
import importlib.util
from typing import Dict, Any

def load_in_json(path_to_json_file: Path) -> Dict[str, Any]:
    with open(path_to_json_file, 'r', encoding='utf-8') as file:
        return json.load(file)

def import_fabric_notebook(notebook_path, module_name):
    """Import a Fabric notebook's Python content"""
    py_file_path = os.path.join(notebook_path, 'notebook-content.py')
   
    spec = importlib.util.spec_from_file_location(module_name, py_file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

test_prefill_data_with_errors = load_in_json(Path(__file__).parent.parent / "data" / "test_virksomheter_prefill_with_uuid_with_errors.json")
test_prefill_data = load_in_json(Path(__file__).parent.parent / "data" / "test_virksomheter_prefill_with_uuid.json")

instance_logging = import_fabric_notebook('clients/instance_logging.Notebook', 'instance_logging')


def test_prefill_valdiation_correct():
    assert True == instance_logging.validate_prefill_data(test_prefill_data_with_errors[0])

@pytest.mark.parametrize("row", test_prefill_data_with_errors[1:])
def test_prefill_validation_fails(row):
    with pytest.raises(instance_logging.PrefillValidationError):
        instance_logging.validate_prefill_data(row)

def test_transform_flat_to_nested_with_prefill_single():
    result = instance_logging.transform_flat_to_nested_with_prefill(test_prefill_data[0])
    expected = {
        "Prefill": {
            "AnsvarligDepartement": {
                "Navn": "KRISTIANSAND",
                "Organisasjonsnummer": "310075728"
            },
            "AnsvarligVirksomhet": {
                "Navn": "KVADRATISK BRA APE",
                "Organisasjonsnummer": "310075728"
            },
            "Kontaktperson": {
                "FulltNavn": "Kontaktperson 1",
                "Telefonnummer": "+(47) 19094980",
                "EPostadresse": "01909498089@testmail.no"
            },
            "Tiltak": {
                "Nummer": "15",
                "Tekst": "15",
                "Kortnavn": "videreutvikle virkemidler for digitalisering og innovasjon i offentlig sektor",
                "ErDeltiltak": True
            },
            "Kapittel": {
                "Nummer": "2.1.4",
                "Tekst": "2.1.4"
            },
            "Maal": {
                "Nummer": "3",
                "Tekst": "3"
            }
        }
    }
    assert result == expected

def test_missing_key_raises_error():
    flat_record = {
        "AnsvarligDepartement.Navn": "A",
    }
    with pytest.raises(KeyError):
        instance_logging.transform_flat_to_nested_with_prefill(flat_record)