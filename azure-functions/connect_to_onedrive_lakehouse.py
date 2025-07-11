import json
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import requests
import os 
import uuid
from dotenv import load_dotenv

def main():
        load_dotenv()
        connection_string = os.getenv("storage_connection_string")
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service.get_container_client("initiellinstancecreated")

        blob_client = container_client.get_blob_client("new_data.json")
        #blob_client.upload_blob(json.dumps({"webhook_id": webhook_id,  "test_data": "matthias tries things"}), overwrite=True)
        credential = DefaultAzureCredential()
        token = credential.get_token("https://api.fabric.microsoft.com/.default").token

        workspace_id = "a9ae54b0-c5c4-4737-aa47-73797fa29580"
        notebook_id = "6e6243d4-70c7-4ca4-83de-d4d508388db7"

        fabric_pipeline_url =    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook"

        headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }

        payload = {
        "executionData": {
                "parameters": {
                "instance_id": {"value": str(uuid.uuid4()), "type": "string"},
                "party_id": {"value": "123456789", "type": "string"}
                }
        }
        }
        response = requests.post(fabric_pipeline_url, headers=headers, json=payload)

        if response.status_code == 202:
                print("Successfully triggered Fabric pipeline.")
        else:
                print(f"Failed to trigger pipeline: {response.status_code}, {response.text}")
  
        print("Successfully uploaded")
if __name__ == "__main__":
    main()

