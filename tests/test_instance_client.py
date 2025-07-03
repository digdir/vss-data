import pytest

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from pathlib import Path
import json
import os
import sys
import importlib
import importlib.util
from typing import Dict, Any
import uuid


def load_in_json(path_to_json_file: Path) -> Dict[str, Any]:
    with open(path_to_json_file, "r", encoding="utf-8") as file:
        return json.load(file)


def import_fabric_notebook(notebook_path, module_name):
    """Import a Fabric notebook's Python content"""
    py_file_path = os.path.join(notebook_path, "notebook-content.py")

    spec = importlib.util.spec_from_file_location(module_name, py_file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


test_prefill_data_with_errors = load_in_json(
    Path(__file__).parent.parent
    / "data"
    / "test_virksomheter_prefill_with_uuid_with_errors.json"
)
test_prefill_data = load_in_json(
    Path(__file__).parent.parent / "data" / "test_virksomheter_prefill_with_uuid.json"
)
maskinport_client = load_in_json(
    Path(__file__).parent.parent / "data" / "maskinporten_config.json"
)
maskinporten_endpoints = load_in_json(
    Path(__file__).parent.parent / "data" / "maskinporten_endpoints.json"
)


credential = DefaultAzureCredential()
client = SecretClient(
    vault_url="https://keyvaultvss.vault.azure.net/", credential=credential
)
secret = client.get_secret("rapdigtest")
secret_value = secret.value

exchange_token_funcs = import_fabric_notebook(
    "auth/exchange_token_funcs.Notebook", "exchange_token_funcs"
)
instance_client = import_fabric_notebook(
    "clients/instance_client.Notebook", "instance_client"
)


def test_update_substatus_success():
    test_config_file = {
        "base_app_url": "https://digdir.apps.tt02.altinn.no",
        "base_platfrom_url": "https://platform.tt02.altinn.no/storage/api/v1/instances",
        "application_owner_organisation": "digdir",
        "appname": "regvil-2025-initiell",
    }
    test_instance_client = instance_client.AltinnInstanceClient.init_from_config(
        test_config_file
    )
    bearer_token = exchange_token_funcs.exchange_token(
        maskinport_client, secret_value, maskinporten_endpoints
    )
    header = {
        "accept": "application/json",
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }
    updated_instance = test_instance_client.update_substatus(
        "51531148",
        "51531148/560d0e53-b034-4994-9dd7-3e1876c23f27",
        "digi-test-uuid",
        header,
    )
    assert updated_instance.status_code == 200
    response_json = updated_instance.json()
    response_json.pop("lastChanged", None)
    assert response_json == {
        "id": "51531148/560d0e53-b034-4994-9dd7-3e1876c23f27",
        "instanceOwner": {
            "partyId": "51531148",
            "personNumber": None,
            "organisationNumber": "310075728",
            "username": None,
        },
        "appId": "digdir/regvil-2025-initiell",
        "org": "digdir",
        "selfLinks": {
            "apps": None,
            "platform": "https://platform.tt02.altinn.no/storage/api/v1/instances/51531148/560d0e53-b034-4994-9dd7-3e1876c23f27",
        },
        "dueBefore": "2025-06-01T12:00:00Z",
        "visibleAfter": "2025-05-20T00:00:00Z",
        "process": {
            "started": "2025-06-24T10:19:20.7812965Z",
            "startEvent": "StartEvent_1",
            "currentTask": {
                "flow": 2,
                "started": "2025-06-24T10:19:20.782519Z",
                "elementId": "Task_1",
                "name": "Utfylling",
                "altinnTaskType": "data",
                "ended": None,
                "validated": None,
                "flowType": "CompleteCurrentMoveToNext",
            },
            "ended": None,
            "endEvent": None,
        },
        "status": {
            "isArchived": False,
            "archived": None,
            "isSoftDeleted": False,
            "softDeleted": None,
            "isHardDeleted": False,
            "hardDeleted": None,
            "readStatus": 1,
            "substatus": {
                "label": "skjema_instance_created",
                "description": '{"digitaliseringstiltak_report_id": "digi-test-uuid"}',
            },
        },
        "completeConfirmations": [
            {"stakeholderId": "digdir", "confirmedOn": "2025-07-02T10:48:15.0400739Z"}
        ],
        "data": [
            {
                "id": "1d82a5fd-98e4-48d9-9e66-6468edb72a54",
                "instanceGuid": "560d0e53-b034-4994-9dd7-3e1876c23f27",
                "dataType": "DataModel",
                "filename": None,
                "contentType": "application/xml",
                "blobStoragePath": "digdir/regvil-2025-initiell/560d0e53-b034-4994-9dd7-3e1876c23f27/data/1d82a5fd-98e4-48d9-9e66-6468edb72a54",
                "selfLinks": {
                    "apps": None,
                    "platform": "https://platform.tt02.altinn.no/storage/api/v1/instances/51531148/560d0e53-b034-4994-9dd7-3e1876c23f27/data/1d82a5fd-98e4-48d9-9e66-6468edb72a54",
                },
                "size": 486,
                "contentHash": None,
                "locked": False,
                "refs": None,
                "isRead": True,
                "tags": [],
                "userDefinedMetadata": None,
                "metadata": None,
                "deleteStatus": None,
                "fileScanResult": "NotApplicable",
                "references": None,
                "created": "2025-06-24T10:19:21.0320742Z",
                "createdBy": "991825827",
                "lastChanged": "2025-06-24T10:19:21.032074Z",
                "lastChangedBy": "991825827",
            }
        ],
        "presentationTexts": {},
        "dataValues": {"dialog.id": "0197a173-0b78-7994-9dd7-3e1876c23f27"},
        "created": "2025-06-24T10:19:20.8243283Z",
        "createdBy": "991825827",
        "lastChangedBy": "991825827",
    }

def test_instance_created_found():
    """Test instance_created returns True when instance exists with matching report_id"""
    test_config_file = {
        "base_app_url": "https://digdir.apps.tt02.altinn.no",
        "base_platfrom_url": "https://platform.tt02.altinn.no/storage/api/v1/instances",
        "application_owner_organisation": "digdir",
        "appname": "regvil-2025-initiell",
    }
    test_instance_client = instance_client.AltinnInstanceClient.init_from_config(
        test_config_file
    )
    bearer_token = exchange_token_funcs.exchange_token(
        maskinport_client, secret_value, maskinporten_endpoints
    )
    header = {
        "accept": "application/json",
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }
    
    # Test with existing instance that should have the report_id
    result = test_instance_client.instance_created(
        header,
        "310075728",  # org_number
        "digi-test-uuid"  # report_id that should exist
    )
    
    assert result is True


def test_instance_created_not_found():
    """Test instance_created returns False when instance doesn't exist"""
    test_config_file = {
        "base_app_url": "https://digdir.apps.tt02.altinn.no",
        "base_platfrom_url": "https://platform.tt02.altinn.no/storage/api/v1/instances",
        "application_owner_organisation": "digdir",
        "appname": "regvil-2025-initiell",
    }
    test_instance_client = instance_client.AltinnInstanceClient.init_from_config(
        test_config_file
    )
    bearer_token = exchange_token_funcs.exchange_token(
        maskinport_client, secret_value, maskinporten_endpoints
    )
    header = {
        "accept": "application/json",
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }
    
    # Test with non-existing report_id
    result = test_instance_client.instance_created(
        header,
        "310075728",  # org_number
        "non-existing-uuid-12345"  # report_id that shouldn't exist
    )
    
    assert result is False


def test_instance_created_different_org():
    """Test instance_created returns False when searching different organisation"""
    test_config_file = {
        "base_app_url": "https://digdir.apps.tt02.altinn.no",
        "base_platfrom_url": "https://platform.tt02.altinn.no/storage/api/v1/instances",
        "application_owner_organisation": "digdir",
        "appname": "regvil-2025-initiell",
    }
    test_instance_client = instance_client.AltinnInstanceClient.init_from_config(
        test_config_file
    )
    bearer_token = exchange_token_funcs.exchange_token(
        maskinport_client, secret_value, maskinporten_endpoints
    )
    header = {
        "accept": "application/json",
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }
    
    # Test with different org number
    result = test_instance_client.instance_created(
        header,
        "999999999",  # Different org_number  
        "digi-test-uuid"  # Same report_id
    )
    
    assert result is False


def test_instance_created_integration():
    """Integration test: Create instance, then verify it exists"""
    test_config_file = {
        "base_app_url": "https://digdir.apps.tt02.altinn.no",
        "base_platfrom_url": "https://platform.tt02.altinn.no/storage/api/v1/instances",
        "application_owner_organisation": "digdir",
        "appname": "regvil-2025-initiell",
    }
    test_instance_client = instance_client.AltinnInstanceClient.init_from_config(
        test_config_file
    )
    bearer_token = exchange_token_funcs.exchange_token(
        maskinport_client, secret_value, maskinporten_endpoints
    )
    header = {
        "accept": "application/json",
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }
    
    # Generate unique test UUID
    test_report_id = str(uuid.uuid4())
    test_org_number = "310075728"
    
    # 1. Verify instance doesn't exist initially
    exists_before = test_instance_client.instance_created(
        header, test_org_number, test_report_id
    )
    assert exists_before is False
    
    # 2. Create new instance
    instance_data = {
        "appId": "digdir/regvil-2025-initiell",    
        "instanceOwner": {
            "personNumber": None,
            "organisationNumber": test_org_number
        },
        "dueBefore": "2025-09-01T12:00:00Z",
        "visibleAfter": "2025-06-29T00:00:00Z"
    }
    
    files = {
        'instance': ('instance.json', json.dumps(instance_data), 'application/json'),
    }
    bearer_token = exchange_token_funcs.exchange_token(
        maskinport_client, secret_value, maskinporten_endpoints
    )
    header = {
        "accept": "application/json",
        "Authorization": f"Bearer {bearer_token}",
    }
    
    created_instance = test_instance_client.post_new_instance(header=header, files=files)
    assert created_instance.status_code == 201
    
    # 3. Update substatus with report_id
    instance_response = created_instance.json()
    instance_id = instance_response["id"]
    party_id = instance_response["instanceOwner"]["partyId"]
    bearer_token = exchange_token_funcs.exchange_token(
        maskinport_client, secret_value, maskinporten_endpoints
    )
    header = {
        "accept": "application/json",
        "Authorization": f"Bearer {bearer_token}",
         "Content-Type": "application/json",
    }
    
    updated_instance = test_instance_client.update_substatus(
        party_id, instance_id, test_report_id, header
    )
    print(updated_instance.json())
    assert updated_instance.status_code == 200
    
    # 4. Verify instance now exists with report_id
    exists_after = test_instance_client.instance_created(
        header, test_org_number, test_report_id
    )
    assert exists_after is True