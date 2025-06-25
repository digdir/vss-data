# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
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
# META     }
# META   }
# META }

# CELL ********************

from typing import Any, Dict
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

example_data = [
{'appId': 'digdir/regvil-2025-initiell', 'instanceOwner': {'personNumber': None, 'organisationNumber': '311138693'}, 'prefill': {'Kontaktperson.FulltNavn': '18846397740@testmail.no', 'AnsvarligDepartement': 'STEINKJER', 'AnsvarligVirksomhet': 'OMKOMMEN TRU TIGER AS', 'Tiltak.Nummer': '92', 'Tiltak.Tekst': '92', 'Tiltak.Kortnavn': 'etablere forskningssentre for utvikling og bruk av KI i samfunnet', 'Tiltak.ErDeltiltak': 'False'}, 'dueBefore': '2025-06-01T12:00:00Z', 'visibleAfter': '2025-05-20T00:00:00Z'},
{'appId': 'digdir/regvil-2025-initiell', 'instanceOwner': {'personNumber': None, 'organisationNumber': '310257265'}, 'prefill': {'Kontaktperson.FulltNavn': '22903147907@testmail.no', 'AnsvarligDepartement': 'ØVRE EIKER', 'AnsvarligVirksomhet': 'USEDVANLIG USELVISK TIGER AS', 'Tiltak.Nummer': '75', 'Tiltak.Tekst': '75', 'Tiltak.Kortnavn': 'prioritere arbeidet med å gjøre tilgjengelig nasjonale datasett som er viktige for offentlig sektor og samfunnet', 'Tiltak.ErDeltiltak': 'False'}, 'dueBefore': '2025-06-01T12:00:00Z', 'visibleAfter': '2025-05-20T00:00:00Z'},
{'appId': 'digdir/regvil-2025-initiell', 'instanceOwner': {'personNumber': None, 'organisationNumber': '311045407'}, 'prefill': {'Kontaktperson.FulltNavn': '01859799503@testmail.no', 'AnsvarligDepartement': 'STAVANGER', 'AnsvarligVirksomhet': 'DEILIG UROKKELIG TIGER AS', 'Tiltak.Nummer': '25', 'Tiltak.Tekst': '25', 'Tiltak.Kortnavn': 'øke sikkerheten og beredskapen i den digitale grunnmuren i sårbare kommuner og regioner gjennom målrettede tilskudd, og vurdere nye tiltak i lys av den endrede sikkerhetspolitiske situasjonen', 'Tiltak.ErDeltiltak': 'False'}, 'dueBefore': '2025-06-01T12:00:00Z', 'visibleAfter': '2025-05-20T00:00:00Z'},
{'appId': 'digdir/regvil-2025-initiell', 'instanceOwner': {'personNumber': None, 'organisationNumber': '310706485'}, 'prefill': {'Kontaktperson.FulltNavn': '02822649540@testmail.no', 'AnsvarligDepartement': 'NES', 'AnsvarligVirksomhet': 'AUTONOM SPESIFIKK APE', 'Tiltak.Nummer': '15', 'Tiltak.Tekst': '15', 'Tiltak.Kortnavn': 'videreutvikle virkemidler for digitalisering og innovasjon i offentlig sektor', 'Tiltak.ErDeltiltak': 'False'}, 'dueBefore': '2025-06-01T12:00:00Z', 'visibleAfter': '2025-05-20T00:00:00Z'},
{'appId': 'digdir/regvil-2025-initiell', 'instanceOwner': {'personNumber': None, 'organisationNumber': '310685232'}, 'prefill': {'Kontaktperson.FulltNavn': '28816898287@testmail.no', 'AnsvarligDepartement': 'OSLO', 'AnsvarligVirksomhet': 'PLUTSELIG FRYKTLØS TIGER AS', 'Tiltak.Nummer': '102', 'Tiltak.Tekst': '102', 'Tiltak.Kortnavn': 'vurdere om låne- og tilskuddsordninger kan innrettes mer mot risikoavlastning for å stimulere til digital innovasjon i næringslivet, særlig for oppstartsbedrifter', 'Tiltak.ErDeltiltak': 'False'}, 'dueBefore': '2025-06-01T12:00:00Z', 'visibleAfter': '2025-05-20T00:00:00Z'},
{'appId': 'digdir/regvil-2025-initiell', 'instanceOwner': {'personNumber': None, 'organisationNumber': '310075728'}, 'prefill': {'Kontaktperson.FulltNavn': '01909498089@testmail.no', 'AnsvarligDepartement': 'KRISTIANSAND', 'AnsvarligVirksomhet': 'KVADRATISK BRA APE', 'Tiltak.Nummer': '15', 'Tiltak.Tekst': '15', 'Tiltak.Kortnavn': 'videreutvikle virkemidler for digitalisering og innovasjon i offentlig sektor', 'Tiltak.ErDeltiltak': 'True'}, 'dueBefore': '2025-06-01T12:00:00Z', 'visibleAfter': '2025-05-20T00:00:00Z'},]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

example_instance = {'id': '51625403/0512ce74-90a9-4b5c-ab15-910f60db92d1',
 'instanceOwner': {'partyId': '51625403',
  'personNumber': None,
  'organisationNumber': '311138693',
  'username': None,
  'party': {'partyId': 51625403,
   'partyUuid': '1ed8aa98-31ed-4f78-b1f6-f12f46e8de04',
   'partyTypeName': 2,
   'ssn': None,
   'orgNumber': '311138693',
   'unitType': 'AS',
   'name': 'OMKOMMEN TRU TIGER AS',
   'isDeleted': False}},
 'appId': 'digdir/regvil-2025-initiell',
 'org': 'digdir',
 'selfLinks': {'apps': 'https://digdir.apps.tt02.altinn.no/digdir/regvil-2025-initiell/instances/51625403/0512ce74-90a9-4b5c-ab15-910f60db92d1',
  'platform': 'https://platform.tt02.altinn.no/storage/api/v1/instances/51625403/0512ce74-90a9-4b5c-ab15-910f60db92d1'},
 'dueBefore': '2025-06-01T12:00:00Z',
 'visibleAfter': '2025-05-20T00:00:00Z',
 'process': {'started': '2025-06-24T10:42:49.5376949Z',
  'startEvent': 'StartEvent_1',
  'currentTask': None,
  'ended': '2025-06-24T10:43:23.1757135Z',
  'endEvent': 'EndEvent_1'},
 'status': {'isArchived': True,
  'archived': '2025-06-24T10:43:23.1757135Z',
  'isSoftDeleted': False,
  'softDeleted': None,
  'isHardDeleted': False,
  'hardDeleted': None,
  'readStatus': 1,
  'substatus': None},
 'completeConfirmations': [{'stakeholderId': 'digdir',
   'confirmedOn': '2025-06-24T13:17:29.8839554Z'}],
 'data': [{'id': 'fed122b9-672c-4b34-9a47-09f501d5af72',
   'instanceGuid': '0512ce74-90a9-4b5c-ab15-910f60db92d1',
   'dataType': 'DataModel',
   'filename': None,
   'contentType': 'application/xml',
   'blobStoragePath': 'digdir/regvil-2025-initiell/0512ce74-90a9-4b5c-ab15-910f60db92d1/data/fed122b9-672c-4b34-9a47-09f501d5af72',
   'selfLinks': {'apps': 'https://digdir.apps.tt02.altinn.no/digdir/regvil-2025-initiell/instances/51625403/0512ce74-90a9-4b5c-ab15-910f60db92d1/data/fed122b9-672c-4b34-9a47-09f501d5af72',
    'platform': 'https://platform.tt02.altinn.no/storage/api/v1/instances/51625403/0512ce74-90a9-4b5c-ab15-910f60db92d1/data/fed122b9-672c-4b34-9a47-09f501d5af72'},
   'size': 830,
   'contentHash': None,
   'locked': True,
   'refs': None,
   'isRead': True,
   'tags': [],
   'userDefinedMetadata': None,
   'metadata': None,
   'deleteStatus': None,
   'fileScanResult': 'NotApplicable',
   'references': None,
   'created': '2025-06-24T10:42:49.5878193Z',
   'createdBy': '991825827',
   'lastChanged': '2025-06-24T10:43:23.253583Z',
   'lastChangedBy': '1260288'},
  {'id': '845875af-abf2-4de8-938b-91cd7b9a2cc9',
   'instanceGuid': '0512ce74-90a9-4b5c-ab15-910f60db92d1',
   'dataType': 'ref-data-as-pdf',
   'filename': 'Rapportering på tiltak i digitaliseringsstrategien.pdf',
   'contentType': 'application/pdf',
   'blobStoragePath': 'digdir/regvil-2025-initiell/0512ce74-90a9-4b5c-ab15-910f60db92d1/data/845875af-abf2-4de8-938b-91cd7b9a2cc9',
   'selfLinks': {'apps': 'https://digdir.apps.tt02.altinn.no/digdir/regvil-2025-initiell/instances/51625403/0512ce74-90a9-4b5c-ab15-910f60db92d1/data/845875af-abf2-4de8-938b-91cd7b9a2cc9',
    'platform': 'https://platform.tt02.altinn.no/storage/api/v1/instances/51625403/0512ce74-90a9-4b5c-ab15-910f60db92d1/data/845875af-abf2-4de8-938b-91cd7b9a2cc9'},
   'size': 44285,
   'contentHash': None,
   'locked': False,
   'refs': None,
   'isRead': True,
   'tags': [],
   'userDefinedMetadata': None,
   'metadata': None,
   'deleteStatus': None,
   'fileScanResult': 'NotApplicable',
   'references': [{'value': 'Task_1',
     'relation': 'GeneratedFrom',
     'valueType': 'Task'}],
   'created': '2025-06-24T10:43:25.6505215Z',
   'createdBy': '1260288',
   'lastChanged': '2025-06-24T10:43:25.650522Z',
   'lastChangedBy': '1260288'}],
 'presentationTexts': {'Tiltaksnavn': 'sørge for en helhetlig og langsiktig prioritering av digitaliseringstiltak i offentlig sektor',
  'Kontaktperson': 'Meg'},
 'dataValues': {'dialog.id': '0197a188-8a48-7b5c-ab15-910f60db92d1'},
 'created': '2025-06-24T10:42:49.5447149Z',
 'createdBy': '991825827',
 'lastChanged': '2025-06-24T13:17:29.883956Z',
 'lastChangedBy': '991825827'}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

class InstanceTracker:
    def __init__(self, log_file: Dict[str, Any]):
        self.log_file = log_file
        self.log_changes = {} 
    
    @classmethod
    def from_log_file(cls, path_to_json_file: str):
        try:
            with open(path_to_json_file, 'r') as f:
                data = json.load(f)
            return cls(data)
        except FileNotFoundError:
            raise FileNotFoundError(f"Log file not found: {path_to_json_file}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in log file: {e}")

    def has_processed_org(self, org_number: str) -> bool:
        return self.log_file.get("organisations", {}).get(org_number) is not None
        
    def logging_instance(self, org_number: str, instance_meta_data: dict, api_response: dict):
        if org_number != instance_meta_data['instanceOwner'].get("organisationNumber"):
            raise ValueError(f"Organization numbers do not match: {org_number} != {instance_meta_data['instanceOwner'].get('organisationNumber')}")
        datamodel_metadata = get_meta_data_info(instance_meta_data["data"])
        self.log_changes[org_number] = {
            "org_number": org_number,
            "org_name": instance_meta_data.get("name"),
            "processed_timestamp": datetime.utcnow().isoformat(),
            "instancePartyId": instance_meta_data['instanceOwner'].get("partyId"),
            "instanceId": instance_meta_data.get("id"),
            "instance_info": {
                "last_changed": instance_meta_data["dataValues"].get('lastChanged'),
                "last_changed_by": instance_meta_data.get('lastChangedBy'),
                "created": instance_meta_data["dataValues"].get('created'),
            },
            "data_info": {
                "last_changed": instance_meta_data["dataValues"].get('lastChanged'),
                "last_changed_by": instance_meta_data.get('lastChangedBy'),
                "created": instance_meta_data["dataValues"].get('created'),
            },
            
            "verification": False,
        }
        
    def update_verification_status(self, org_number: str, 
                                 api_verification: bool):
        self.log_changes[org_number]["verification"] = True
                            

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

from typing import List, Dict, Optional
def get_meta_data_info(list_of_data_instance_meta_info: List[Dict[str, str]]) -> Dict[str, str]:
    if not list_of_data_instance_meta_info:
        raise ValueError("No instance metadata provided.")

    for instance in list_of_data_instance_meta_info:
        if (
            instance.get("dataType") == "DataModel" and 
            instance.get("contentType") in ["application/xml", "application/json"]
        ):
            return instance

    raise ValueError("No instance with dataType='DataModel' and contentType='application/xml' or 'application/json' was found.")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

#GET INSTANCE RESPONSE
instance = [{'id': '51101237/5a700bc2-2af3-484a-b3bd-dd347aa40484',
  'presentationTexts': None,
  'dueBefore': None,
  'lastChanged': '2025-04-12T14:09:52.555628Z',
  'lastChangedBy': 'KETSJUP LYSTIG'}]

prefill = {"regjeringen vil": "15","Ansvarlig dept": "KRISTIANSAND", "organisasjonsnummer": "310075728",
"nav": "KVADRATISK BRA APE","kontaktperson": "01909498089","epost_kontaktperson": "01909498089@testmail.no"}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

data_model_schema = {
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "http://altinn-repositories:3000/digdir/regvil-2025-initiell/App/models/DataModel.schema.json",
  "info": {
    "rootNode": ""
  },
  "@xsdNamespaces": {
    "xsd": "http://www.w3.org/2001/XMLSchema",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "seres": "http://seres.no/xsd/forvaltningsdata"
  },
  "@xsdSchemaAttributes": {
    "AttributeFormDefault": "Unqualified",
    "ElementFormDefault": "Qualified",
    "BlockDefault": "None",
    "FinalDefault": "None"
  },
  "@xsdRootElement": "DataModel",
  "type": "object",
  "$defs": {
    "Kontaktperson": {
      "type": "object",
      "properties": {
        "FulltNavn": {
          "type": "string",
          "maxLength": 5000
        },
        "Telefonnummer": {
          "type": "string",
          "maxLength": 5000
        },
        "EPostadresse": {
          "type": "string",
          "format": "email",
          "maxLength": 5000
        }
      }
    },
    "AndreVirksomheter": {
      "type": "object",
      "properties": {
        "Navn": {
          "type": "string",
          "maxLength": 5000
        }
      }
    },
    "Valg": {
      "type": "object",
      "properties": {
        "Nummer": {
          "type": "string",
          "maxLength": 5000
        },
        "Tekst": {
          "type": "string",
          "maxLength": 5000
        }
      }
    },
    "Tiltak": {
      "type": "object",
      "properties": {
        "Nummer": {
          "type": "string",
          "maxLength": 5000
        },
        "Tekst": {
          "type": "string",
          "maxLength": 5000
        },
        "Kortnavn": {
          "type": "string",
          "maxLength": 5000
        },
        "ErDeltiltak": {
          "type": "boolean"
        },
        "Label": {
          "type": "string",
          "maxLength": 5000
        }
      }
    }
  },
  "properties": {
    "AnsvarligDepartement": {
      "type": "string",
      "maxLength": 5000
    },
    "AnsvarligVirksomhet": {
      "type": "string",
      "maxLength": 5000
    },
    "Kontaktperson": {
      "$ref": "#/$defs/Kontaktperson"
    },
    "AndreKontaktpersoner": {
      "type": "array",
      "items": {
        "$ref": "#/$defs/Kontaktperson"
      }
    },
    "AndreVirksomheter": {
      "type": "array",
      "items": {
        "$ref": "#/$defs/AndreVirksomheter"
      }
    },
    "Tiltak": {
      "$ref": "#/$defs/Tiltak"
    },
    "Kapittel": {
      "$ref": "#/$defs/Valg"
    },
    "Maal": {
      "$ref": "#/$defs/Valg"
    },
    "AndreMaal": {
      "type": "array",
      "items": {
        "$ref": "#/$defs/Valg"
      }
    },
    "ErTiltaketPaabegynt": {
      "type": "boolean"
    },
    "DatoPaabegynt": {
      "type": "string",
      "maxLength": 5000
    },
    "VetOppstartsDato": {
      "type": "boolean"
    },
    "DatoForventetOppstart": {
      "type": "string",
      "maxLength": 5000
    }
  }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

from typing import Dict, List, Any

def extract_string_fields(definition: Dict[str, Any], prefix: str = "") -> List[str]:
    fields = []
    if definition.get("type") == "object" and "properties" in definition:
        for prop_name, prop_def in definition["properties"].items():
            full_path = f"{prefix}.{prop_name}" if prefix else prop_name
            if prop_def.get("type") == "string":
                fields.append(full_path)
            elif prop_def.get("type") == "object":
                fields.extend(extract_string_fields(prop_def, full_path))
    return fields

def get_datamodel_prefill_structure(datamodel_structure: Dict[str, Any]) -> List[str]:
    prefill_structure = []
    defs = datamodel_structure.get("$defs")

    for def_name, def_value in defs.items():
        prefill_structure.extend(extract_string_fields(def_value, def_name))
    
    return prefill_structure


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

prefill_structure = get_datamodel_prefill_structure(data_model_schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

prefill_structure

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
