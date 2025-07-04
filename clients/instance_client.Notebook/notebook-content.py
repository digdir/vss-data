# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9fbcae95-6f3d-4f21-96dd-b0a98150a56d",
# META       "default_lakehouse_name": "RegVil_Lakehouse",
# META       "default_lakehouse_workspace_id": "a9ae54b0-c5c4-4737-aa47-73797fa29580",
# META       "known_lakehouses": [
# META         {
# META           "id": "9fbcae95-6f3d-4f21-96dd-b0a98150a56d"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "cfc485be-bcb5-412e-9356-f6b9743f7fd5",
# META       "workspaceId": "5cce3fd5-2dac-41a7-a0e4-acfd383bdb8f"
# META     }
# META   }
# META }

# CELL ********************
from functools import wraps
from typing import Callable, Dict, Optional, Tuple, List
import requests
import logging
import json
import uuid
from datetime import datetime
from unittest.mock import Mock
import os
import importlib
import sys


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def import_fabric_notebook(notebook_path, module_name):
    """Import a Fabric notebook's Python content"""
    py_file_path = os.path.join(notebook_path, 'notebook-content.py')
   
    spec = importlib.util.spec_from_file_location(module_name, py_file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module
 
# Import your notebooks
exchange_token_funcs = import_fabric_notebook('auth/exchange_token_funcs.Notebook', 'exchange_token_funcs')

def get_meta_data_info(list_of_data_instance_meta_info: List[Dict[str, str]]) -> Dict[str, str]:
    if not list_of_data_instance_meta_info:
        raise ValueError("No instance metadata provided.")

    for instance in list_of_data_instance_meta_info:
        if (
            instance.get("dataType") in ["DataModel", "model"] and 
            instance.get("contentType") in ["application/xml", "application/json"]
        ):
            return instance
        else:
            continue

    raise ValueError("No instance with dataType='DataModel' and contentType='application/xml' or 'application/json' was found.")

def extract_instances_ids(data_storage_extract):
    instances = []
    for instance in data_storage_extract["instances"]:
        if instance.get("data", []):
            instance_data_meta_data = get_meta_data_info(instance["data"])


            instances.append(
            {"instanceOwnerPartyId": instance["instanceOwner"]["partyId"], 
            "organisationNumber": instance["instanceOwner"].get("organisationNumber", ""), 
            "personNumber": instance["instanceOwner"].get("personNumber", ""),
            "instanceId": instance["id"], 
            "dataGuid": instance_data_meta_data.get("id"),
            "tags": instance_data_meta_data.get("tags", [])}
        )
    return instances

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_default_headers(bearer_token: str) -> Dict[str, str]:
    return {
            "accept": "application/json",
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json"
        }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def make_api_call(method: str, url: str, headers: Dict[str, str], data: Optional[Dict[str, str]] = None, params: Optional[Dict[str, str]] = None, files: Optional[Dict[str, str]] = None) -> Optional[requests.Response]:
    try:
        response = requests.request(method, url, headers=headers, data=data, params=params, files=files)
            
        if response.status_code in [200, 201, 204]:  # Success codes
            logging.info(f"API call successful: {method} {url}")
            return response
        elif response.status_code == 404:
            logging.warning(f"Resource not found: {method} {url}")
        elif response.status_code == 403:
            logging.warning(f"Access denied: {method} {url}")
        elif response.status_code == 401:
            logging.warning(f"Unauthorized access - check authentication token")
        else:
            logging.warning(f"API call failed with status {response.status_code}: {response.text}")
        return None
            
    except requests.exceptions.ConnectionError:
        logging.error(f"Connection error when calling {url}")
        return None
    except requests.exceptions.Timeout:
        logging.error(f"Timeout when calling {url}")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {str(e)}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error in API call: {str(e)}")
        return None


def generate_mock_guid() -> str:
    return str(uuid.uuid4())

def mock_update_substatus(instanceOwnerPartyId: str, instanceGuid: str, digitaliseringstiltak_report_id: str) -> Mock:
    now_iso = datetime.utcnow().isoformat() + "Z"
    # Construct mock response data
    response_data = {
        "instanceOwner": {
            "partyId": instanceOwnerPartyId
        },
        "id": f"{instanceOwnerPartyId}/{instanceGuid}",
        "status": {
            "substatus": {
                "label": "skjema_instance_created",
                "description": {"digitaliseringstiltak_report_id": digitaliseringstiltak_report_id}
            }
        },
        "lastChanged": now_iso,
        "lastChangedBy": "991825827"
    }

    mock_response = Mock()
    mock_response.status_code = 200  # OK
    mock_response.json.return_value = response_data
    mock_response.text = json.dumps(response_data)
    mock_response.headers = {
        "Content-Type": "application/json"
    }
    mock_response.ok = True
    mock_response.reason = "OK"
    return mock_response


def mock_post_new_instance(header: Dict[str, str], files: Dict[str, Tuple[str, str, str]]) -> Dict:
    # Parse instance and datamodel JSONs
    instance_content = json.loads(files["instance"][1])
    datamodel_content = json.loads(files["DataModel"][1])

    now_iso = datetime.utcnow().isoformat() + "Z"
    instance_guid = generate_mock_guid()
    party_id = "51625403"  # Simulated party ID
    org_number = instance_content["instanceOwner"]["organisationNumber"]

    response_data = {
        "id": f"{party_id}/{instance_guid}",
        "instanceOwner": {
            "partyId": party_id,
            "organisationNumber": org_number,
            "party": {
                "partyId": party_id,
                "partyUuid": generate_mock_guid(),
                "partyTypeName": 2,
                "orgNumber": org_number,
                "unitType": "AS",
                "name": datamodel_content.get("Prefill", {}).get("AnsvarligVirksomhet", {}).get("Navn", None),
                "isDeleted": False
            }
        },
        "appId": instance_content.get("appId"),
        "org": instance_content.get("appId").split("/")[0],
        "dueBefore": instance_content.get("dueBefore"),
        "visibleAfter": instance_content.get("visibleAfter"),
        "status": {
            "isArchived": False,
            "isSoftDeleted": False,
            "isHardDeleted": False,
            "readStatus": 1,
            "substatus": None
        },
        "lastChangedBy": "991825827",
        "created": now_iso,
        "lastChanged": now_iso,
        "data": [
            {
                "id": generate_mock_guid(),
                "instanceGuid": instance_guid,
                "dataType": "DataModel",
                "contentType": "application/json",
                "created": now_iso,
                "lastChanged": now_iso,
                "lastChangedBy": "991825827"
            }
        ]
    }

        # Create mock response object
    mock_response = Mock()
    mock_response.status_code = 201  # Created
    mock_response.json.return_value = response_data
    mock_response.text = json.dumps(response_data)
    mock_response.headers = {
        "Content-Type": "application/json",
        "Location": f"/instances/{party_id}/{instance_guid}"
    }
    mock_response.ok = True
    mock_response.reason = "Created"
    
    return mock_response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class AltinnInstanceClient:

    def __init__(self, base_app_url: str, base_platfrom_url: str,  application_owner_organisation: str, appname: str, maskinport_client: str, secret_value: str, maskinporten_endpoint: str):
        self.base_app_url = base_app_url
        self.base_platfrom_url = base_platfrom_url
        self.application_owner_organisation = application_owner_organisation
        self.appname = appname
        self.basePathApp = f"{self.base_app_url}/{self.application_owner_organisation}/{self.appname}/instances"
        self.basePathPlatform = f"{self.base_platfrom_url}"
                # Add token management
        self.maskinport_client = maskinport_client
        self.secret_value = secret_value
        self.maskinporten_endpoint = maskinporten_endpoint

    def _get_headers(self, content_type=None):
        """Get fresh headers with new token"""
        token = exchange_token_funcs.exchange_token(
            self.maskinport_client, 
            self.secret_value, 
            self.maskinporten_endpoint
        )
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {token}"
        }
        if content_type:
            headers["Content-Type"] = content_type
        return headers

    @classmethod
    def init_from_config(cls, app_config_file: Dict[str, str], maskinport_config_file: Dict[str, str]):
        return cls(
            base_app_url=app_config_file["base_app_url"],
            base_platfrom_url=app_config_file["base_platfrom_url"],
            application_owner_organisation=app_config_file["application_owner_organisation"], 
            appname=app_config_file["appname"], 
            maskinport_client=maskinport_config_file["maskinport_client"],
            secret_value=maskinport_config_file["secret_value"], 
            maskinporten_endpoint=maskinport_config_file["maskinporten_endpoint"]
        )
    
    def get_instance(self, instanceOwnerPartyId: str, instanceGuid: str, header: Optional[Dict[str, str]] = None) -> Optional[requests.Response]:
        instance_id = instanceGuid.split("/")[1]
        url = f"{self.basePathApp}/{instanceOwnerPartyId}/{instance_id}"
        return make_api_call(method="GET", url=url, headers=self._get_headers("application/json"))
    
    def get_active_instance(self, instanceOwnerPartyId: str, header: Optional[Dict[str, str]] = None) -> Optional[requests.Response]:
        url = f"{self.basePathApp}/{instanceOwnerPartyId}/active"
        return make_api_call(method="GET", url=url, headers=self._get_headers("application/json"))

    def post_new_instance(self, files: Dict[str, Tuple[str, str, str]], header: Optional[Dict[str, str]] = None) -> Optional[requests.Response]:
        url = f"{self.basePathApp}"
        return make_api_call(method="POST", url=url, headers=self._get_headers(), files=files)
    
    def mock_test_post_new_instance(self,  files: Dict[str, Tuple[str, str, str]], header: Optional[Dict[str, str]] = None) -> Dict:
        """Simulates an API response from Altinn after posting a new instance."""
        return mock_post_new_instance(header, files)
        
    def get_stored_instances_ids(self, header: Optional[Dict[str, str]] = None):
        url = f"{self.base_platfrom_url}"
        params = {
        'org': self.application_owner_organisation,
        'appId': f"{self.application_owner_organisation}/{self.appname}"
        }
        data_storage_instances = make_api_call(method="GET", url=url, headers=self._get_headers("application/json"), params=params)
        return extract_instances_ids(data_storage_instances.json())

    def instance_created(self, org_number: str, tag: str, header: Optional[Dict[str, str]] = None) -> bool:
        stored_instances = self.get_stored_instances_ids(self._get_headers("application/json"))
        for instance in stored_instances:
            if instance.get("organisationNumber") != org_number:
                continue
            if tag in instance.get("tags"):
                return True
        return False
    
    def complete_instance(self, instanceOwnerPartyId: str, instanceGuid: str, header: Optional[Dict[str, str]] = None) -> Optional[requests.Response]:
        instance_id = instanceGuid.split("/")[1]
        url = f"{self.basePathApp}/{instanceOwnerPartyId}/{instance_id}/complete"
        return make_api_call(method="POST", url=url, headers=self._get_headers("application/json"))
    
    def update_substatus(self, instanceOwnerPartyId: str, instanceGuid: str, digitaliseringstiltak_report_id: str, header: Optional[Dict[str, str]] = None) -> Optional[requests.Response]:
        instance_id = instanceGuid.split("/")[1]
        url = f"{self.basePathApp}/{instanceOwnerPartyId}/{instance_id}/substatus"
        payload = {
            "label": "skjema_instance_created",
            "description": json.dumps({"digitaliseringstiltak_report_id": digitaliseringstiltak_report_id})
        }
        return make_api_call(method="PUT", url=url, headers=self._get_headers(), data=json.dumps(payload))
    
    def tag_instance_data(self, instanceOwnerPartyId: str, instanceGuid: str, dataGuid: str, tag: str, header: Optional[Dict[str, str]] = None) -> Optional[requests.Response]:
        instance_id = instanceGuid.split("/")[1]
        url = f"{self.basePathApp}/{instanceOwnerPartyId}/{instance_id}/data/{dataGuid}/tags"
        print(url)
        return make_api_call(method="POST", url=url, headers=self._get_headers("application/json"), data=json.dumps(tag))
    
    def mock_test_update_substatus(self, instanceOwnerPartyId: str, instanceGuid: str, digitaliseringstiltak_report_id: str, header: Optional[Dict[str, str]] = None):
        return mock_update_substatus(instanceOwnerPartyId, instanceGuid, digitaliseringstiltak_report_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
