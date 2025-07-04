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

%run exchange_token_funcs 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run instance_client

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from typing import Any, Dict
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential 
import json
from pathlib import Path
import pandas as pd

def load_in_json(path_to_json_file):
    with open(path_to_json_file, 'r', encoding='utf-8') as file:
        return json.load(file)

credential = DefaultAzureCredential()
client = SecretClient(vault_url="https://keyvaultvss.vault.azure.net/", credential=credential)
secret = client.get_secret("rapdigtest")
secret_value = secret.value

maskinport_client = {
    "kid":"RapDig_maskinporten-TEST.2025-05-22",
    "client_id":"387ff46c-222f-412b-ab63-3abf0a2704bc",
    "scope":"altinn:serviceowner/instances.read altinn:serviceowner/instances.write altinn:serviceowner",
}

maskinporten_endpoints = {
    "prod": "https://maskinporten.no/",
    "test": "https://test.maskinporten.no/",
    "ver1": "https://ver1.maskinporten.no/",
    "ver2": "https://ver2.maskinporten.no/"
}

def fill_post_data(prefill_data: Dict[str, Any]):
    for _, prefill_data_row in prefill_data.items():
        data = {"appId" : "digdir/regvil-2025-initiell",    
        "instanceOwner": {"personNumber": None,
        'organisationNumber': prefill_data_row["organisasjonsnummer"]},
        "prefill":{
        "Kontaktperson.FulltNavn": prefill_data_row["kontaktperson"],
        "Kontaktperson.Epost": prefill_data_row["epost_kontaktperson"],
        "AnsvarligDepartement": prefill_data_row["Ansvarlig dept"],
        "AnsvarligVirksomhet": prefill_data_row["navn"],
        "Tiltak.Nummer":prefill_data_row["tiltak"],
        "Tiltak.Tekst": prefill_data_row["tiltak"],
        "Tiltak.Kortnavn": prefill_data_row["Kortnavn"],
        "Tiltak.ErDeltiltak": prefill_data_row["ErDeltiltak"],
        },
        "dueBefore":"2025-06-01T12:00:00Z",
        "visibleAfter": "",
        }
        return data

config_client_file = {
    "base_app_url": "https://digdir.apps.tt02.altinn.no", 
    "base_platfrom_url": "https://platform.tt02.altinn.no/storage/api/v1/instances",
    "application_owner_organisation": "digdir", 
    "appname": "regvil-2025-initiell"
}

def main():
    app_instance_client = AltinnInstanceClient.init_from_config(config_client_file)
    bearer_token = exchange_token(maskinport_client, secret_value, maskinporten_endpoints) 
    header={"accept": "application/json","Authorization":f"Bearer {bearer_token}", "Content-Type":"application/json" }
    instances = app_instance_client.get_stored_instances_ids(header)
    bearer_token = exchange_token(maskinport_client, secret_value, maskinporten_endpoints) 
    header={"accept": "application/json","Authorization":f"Bearer {bearer_token}", "Content-Type":"application/json" }
    active_instances = app_instance_client.get_instance(instances[0]["instanceOwnerPartyId"], instances[0]["instanceId"], header)
    print(active_instances.json())

if __name__ == "__main__":
    main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
