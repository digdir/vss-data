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

#%run exchange_token_funcs 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#%run instance_client

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from typing import Any, Dict
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential 
from pathlib import Path
import json
import os
import sys
import importlib
import importlib.util
import requests

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
instance_client = import_fabric_notebook('clients/instance_client.Notebook', 'instance_client')
instance_logging = import_fabric_notebook('clients/instance_logging.Notebook', 'instance_logging')


credential = DefaultAzureCredential()
client = SecretClient(vault_url="https://keyvaultvss.vault.azure.net/", credential=credential)
secret = client.get_secret("rapdigtest")
secret_value = secret.value

def load_in_json(path_to_json_file: Path) -> Dict[str, Any]:
    with open(path_to_json_file, 'r', encoding='utf-8') as file:
        return json.load(file)

def main():
    maskinport_client = load_in_json(Path(__file__).parent.parent / "data" / "maskinporten_config.json")
    maskinporten_endpoints = load_in_json(Path(__file__).parent.parent / "data" / "maskinporten_endpoints.json")
    test_config_client_file = load_in_json(Path(__file__).parent.parent / "data" / "test_config_client_file.json")
    test_prefill_data = load_in_json(Path(__file__).parent.parent / "data" / "test_virksomheter_prefill_with_uuid.json")
    bearer_token = exchange_token_funcs.exchange_token(maskinport_client, secret_value, maskinporten_endpoints) 
    header = {
                "accept": "application/json",
                "Authorization": f"Bearer {bearer_token}",
            }
    regvil_instance_client = instance_client.AltinnInstanceClient.init_from_config(test_config_client_file)

    tracker = instance_logging.InstanceTracker.from_log_file(Path(__file__).parent.parent / "data" / "instance_log" / "instance_log.json")

    for prefill_data_row in test_prefill_data[:1]:
        instance_logging.validate_prefill_data(prefill_data_row)

        data_model = instance_logging.transform_flat_to_nested_with_prefill(prefill_data_row)

        if not tracker.has_processed_instance(prefill_data_row["AnsvarligVirksomhet.Organisasjonsnummer"], prefill_data_row["digitaliseringstiltak_report_id"]):
            instance_data = {"appId" : "digdir/regvil-2025-initiell",    
            "instanceOwner": {"personNumber": None,
            "organisationNumber": data_model["Prefill"]["AnsvarligVirksomhet"]["Organisasjonsnummer"]},
            "dueBefore":"2025-09-01T12:00:00Z",
            "visibleAfter": "2025-06-29T00:00:00Z"
            }
            files = {
                'instance': ('instance.json', json.dumps(instance_data), 'application/json'),
                'DataModel': ('datamodel.json', json.dumps(data_model), 'application/json')
            }
            bearer_token = exchange_token_funcs.exchange_token(maskinport_client, secret_value, maskinporten_endpoints) 
            header = {
                "accept": "application/json",
                "Authorization": f"Bearer {bearer_token}",
                "Content-Type": "application/json"
            }
            created_instance = regvil_instance_client.mock_test_post_new_instance(header, files)
            instance_meta_data = created_instance.json()
            updated_instance = regvil_instance_client.mock_test_update_substatus(header, instance_meta_data["instanceOwner"]["partyId"], instance_meta_data["id"], prefill_data_row["digitaliseringstiltak_report_id"])

            bearer_token = exchange_token_funcs.exchange_token(maskinport_client, secret_value, maskinporten_endpoints) 
            header = {
                "accept": "application/json",
                "Authorization": f"Bearer {bearer_token}",
                "Content-Type": "application/json"
            }
            updated_instance = regvil_instance_client.mock_test_update_substatus(instance_meta_data["instanceOwner"]["partyId"], instance_meta_data["id"], prefill_data_row["digitaliseringstiltak_report_id"], header)
            #updated_instance = regvil_instance_client.update_substatus("51531148", "51531148/560d0e53-b034-4994-9dd7-3e1876c23f27", prefill_data_row["digitaliseringstiltak_report_id"], header)
            print("=====================================================================")
            print(updated_instance.status_code)
            print(updated_instance.json())
            #if created_instance.status_code == 201:
            #    tracker.logging_instance(prefill_data_row["AnsvarligVirksomhet.Organisasjonsnummer"], prefill_data_row["digitaliseringstiltak_report_id"], created_instance.json())
            #    tracker.save_to_disk()
            #else:
            #    try:
            #        error_details = created_instance.json()
            #        error_msg = error_details.get('error', 'Unknown error')
            #    except:
            #        error_msg = created_instance.text or 'No error details'

            #    logging.warning(f"API Error: Org {prefill_data_row['AnsvarligVirksomhet.Organisasjonsnummer']}, "
            #                    f"Report {prefill_data_row['digitaliseringstiltak_report_id']} - "
            #                    f"Status: {created_instance.status_code} - "
            #                    f"Error message: {error_msg}")
        else:
            print("Not created")

if __name__ == "__main__":
    main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }