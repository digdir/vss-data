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

from typing import Dict, Union, Optional, List
import requests
import logging

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_instances_ids(data_storage_extract):
    instances = []
    for instance in data_storage_extract["instances"]:
        instances.append(
            {"instanceOwnerPartyId": instance["instanceOwner"]["partyId"], 
            "organisationNumber": instance["instanceOwner"].get("organisationNumber", ""), 
            "personNumber": instance["instanceOwner"].get("personNumber", ""),
            "instanceId": instance["id"]}
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

def make_api_call(method: str, url: str, headers: Dict[str, str], data: Optional[Dict[str, str]] = None, params: Optional[Dict[str, str]] = None) -> Optional[requests.Response]:
    try:
        response = requests.request(method, url, headers=headers, data=data, params=params)
            
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class AltinnInstanceClient:

    def __init__(self, base_app_url: str, base_platfrom_url: str,  application_owner_organisation: str, appname: str):
        self.base_app_url = base_app_url
        self.base_platfrom_url = base_platfrom_url
        self.application_owner_organisation = application_owner_organisation
        self.appname = appname
        self.basePathApp = f"{self.base_app_url}/{self.application_owner_organisation}/{self.appname}/instances"
        self.basePathPlatform = f"{self.base_platfrom_url}"

    @classmethod
    def init_from_config(cls, config_file: Dict[str, str]):
        return cls(
            base_app_url=config_file["base_app_url"],
            base_platfrom_url=config_file["base_platfrom_url"],
            application_owner_organisation=config_file["application_owner_organisation"], 
            appname=config_file["appname"]
        )
    
    def get_instance(self, instanceOwnerPartyId: str, instanceGuid: str, header: Dict[str, str]) -> Optional[requests.Response]:
        instance_id = instanceGuid.split("/")[1]
        print(instanceOwnerPartyId)
        print(instance_id)
        url = f"{self.basePathApp}/{instanceOwnerPartyId}/{instance_id}"
        print(url)
        return make_api_call(method="GET", url=url, headers=header)
    
    def get_active_instance(self, instanceOwnerPartyId: str, header: Dict[str, str]) -> Optional[requests.Response]:
        url = f"{self.basePathApp}/{instanceOwnerPartyId}/active"
        return make_api_call(method="GET", url=url, headers=header)
    
    def post_new_instance(self, instanceOwnerPartyId: str, header: Dict[str, str], data: Dict[str, str]) -> Optional[requests.Response]:
        url = f"{self.basePathApp}/create"
        return make_api_call(method="POST", url=url, headers=header, data=data)
    
    def get_stored_instances_ids(self, header: Dict[str, str]):
        url = f"{self.base_platfrom_url}"
        params = {
        'org': self.application_owner_organisation,
        'appId': f"{self.application_owner_organisation}/{self.appname}"
        }
        data_storage_instances = make_api_call(method="GET", url=url, headers=header, params=params)
        return extract_instances_ids(data_storage_instances.json())

    def complete_instance(self, instanceOwnerPartyId: str, instanceGuid: str, header: Dict[str, str]) -> Optional[requests.Response]:
        instance_id = instanceGuid.split("/")[1]
        url = f"{self.basePathApp}/{instanceOwnerPartyId}/{instance_id}/complete"
        return make_api_call(method="POST", url=url, headers=header)
    
    
    def delete_instance(self, instanceOwnerPartyId: str, instanceGuid: str, header: Dict[str, str], hard_delete: bool = False) -> Optional[requests.Response]:
        instance_id = instanceGuid.split("/")[1]
        url = f"{self.basePathApp}/{instanceOwnerPartyId}/{instance_id}"
        return make_api_call(method="DELETE", url=url, headers=header, params={"hard": str(hard_delete).lower()})



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
