import azure.functions as func
import logging
from typing import Dict
from azure.identity import DefaultAzureCredential
import requests
import os

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def get_fabric_token():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    return token

def trigger_fabric_notebook(token: str, notebook_id: str, workspace_id: str, instance_id: str, party_id: str):
    fabric_pipeline_url = (
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
        f"/items/{notebook_id}/jobs/instances?jobType=RunNotebook"
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "executionData": {
            "parameters": {
                "instance_id": {"value": instance_id, "type": "string"},
                "party_id": {"value": party_id, "type": "string"}
            }
        }
    }

    response = requests.post(fabric_pipeline_url, headers=headers, json=payload)
    return response

def extract_ids_from_source(source_url: str):
    parts = source_url.split("/")
    return parts[-1], parts[-2]  # instance_id, party_id

@app.route(route="httppost", methods=["POST"])
def http_post(req: func.HttpRequest) -> func.HttpResponse:
    try:
        event = req.get_json()
        logging.info(f"Event type: {event.get('type')}")
        source_url = event.get('source')
        instance_id, party_id = extract_ids_from_source(source_url)
        
        logging.info(f"Party ID: {party_id}, Instance ID: {instance_id}")

        if event.get("type") == "app.instance.process.completed":
            notebook_id = os.environ["NOTEBOOK_ID"]
            workspace_id = os.environ["WORKSPACE_ID"]

            token = get_fabric_token()
            response = trigger_fabric_notebook(token, notebook_id, workspace_id, instance_id, party_id)

            if response.status_code == 202:
                return func.HttpResponse("Successfully triggered Fabric pipeline.")
            else:
                return func.HttpResponse(f"Failed to trigger pipeline: {response.status_code}, {response.text}", status_code=500)

        return func.HttpResponse("Event received", status_code=200)

    except ValueError:
        return func.HttpResponse("Invalid JSON in request body", status_code=400)
    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(f"Internal Server Error: {str(e)}", status_code=500)
