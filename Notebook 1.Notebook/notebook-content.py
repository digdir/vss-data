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

from notebookutils.mssparkutils.credentials import getSecret

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

secret = getSecret("https://keyvaultvss.vault.azure.net/", "rapdigtest")
client = spark.read.option("multiline", "true").json("abfss://a9ae54b0-c5c4-4737-aa47-73797fa29580@onelake.dfs.fabric.microsoft.com/9fbcae95-6f3d-4f21-96dd-b0a98150a56d/Files/test/client_config.json").collect()[0].asDict()
maskinporten_endpoints = spark.read.option("multiline", "true").json("abfss://a9ae54b0-c5c4-4737-aa47-73797fa29580@onelake.dfs.fabric.microsoft.com/9fbcae95-6f3d-4f21-96dd-b0a98150a56d/Files/test/maskinporten_enpoints.json").collect()[0].asDict()
test_prefill = spark.read.format("csv").option("header","true").option("delimiter", ";").load("Files/test/test_virksomheter_prefill.csv").collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for prefill_data_row in test_prefill:
    data = {"appId" : "digdir/regvil-2025-initiell",    
    "instanceOwner": {"personNumber": None,
    'organisationNumber': prefill_data_row["organisasjonsnummer"]},
     #"instantiationInstance": {},  
    "prefill":{
    "Kontaktperson.FulltNavn": prefill_data_row["kontaktperson"],
    "Kontaktperson.FulltNavn": prefill_data_row["epost_kontaktperson"],
    "AnsvarligDepartement": prefill_data_row["Ansvarlig dept"],
    "AnsvarligVirksomhet": prefill_data_row["navn"],
     "Tiltak.Nummer":prefill_data_row["tiltak"],
    "Tiltak.Tekst": prefill_data_row["tiltak"],
    "Tiltak.Kortnavn": prefill_data_row["Kortnavn"],
    "Tiltak.ErDeltiltak": prefill_data_row["ErDeltiltak"],
    },
    "dueBefore":"2025-06-01T12:00:00Z", #Dato til alle?
    "visibleAfter": "",
     }
    print(data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

config_client_file = {
    "base_app_url": "https://digdir.apps.tt02.altinn.no", 
    "base_platfrom_url": "https://platform.tt02.altinn.no/storage/api/v1/instances",
    "application_owner_organisation": "digdir", 
    "appname": "regvil-2025-initiell"
}

app_instance_client = AltinnInstanceClient.init_from_config(config_client_file)
bearer_token = exchange_token(client, maskinporten_endpoints) 
header={"accept": "application/json","Authorization":f"Bearer {bearer_token}", "Content-Type":"application/json" }
instances = app_instance_client.get_stored_instances_ids(header)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bearer_token = exchange_token(client, maskinporten_endpoints) 
header={"accept": "application/json","Authorization":f"Bearer {bearer_token}", "Content-Type":"application/json" }
active_instances = app_instance_client.get_instance(instances[0]["instanceOwnerPartyId"], instances[0]["instanceId"], header)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

active_instances.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bearer_token = exchange_token(client, maskinporten_endpoints) 
header={"accept": "application/json","Authorization":f"Bearer {bearer_token}", "Content-Type":"application/json" }
active_instances = app_instance_client.get_instance(instances[0]["instanceOwnerPartyId"], instances[0]["instanceId"], header)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

active_instances.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data= json.dumps({"appId" : "digdir/regvil-2025-initiell",    
    "instanceOwner": {"personNumber": None,
    'organisationNumber': '310075728'},
     #"instantiationInstance": {},  
    "prefill":{
    "Kontaktperson.FulltNavn": "Andreas Andreasen",
    "Kontaktperson.FulltNavn": "Andreas.Andreasen@virksomhet.no",
    "AnsvarligDepartement": "Andreas Departmenent", 
    "AnsvarligVirksomhet": "Andreas Virksomhet", 
     "Tiltak.Nummer": "3",
    "Tiltak.Tekst": "3",
    "Tiltak.Kortnavn": "X",
    "Tiltak.ErDeltiltak": "False"
    },
    "dueBefore":"2025-06-01T12:00:00Z",
    "visibleAfter": "2025-05-20T00:00:00Z",
     })
instance_post = requests.post("https://digdir.apps.tt02.altinn.no/digdir/regvil-2025-initiell/instances/create",
    headers={"accept": "application/json","Authorization":f"Bearer {exchange_token()}", "Content-Type":"application/json" }, 
    data= data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def main():
    print("This works")
    return None

if __name__ == "__main__":
    main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
