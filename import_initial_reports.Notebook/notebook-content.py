# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c232be5e-59da-4a9b-87df-e4b5307f5fce",
# META       "default_lakehouse_name": "ProjectPortfolio",
# META       "default_lakehouse_workspace_id": "4049da66-dd02-4708-a98e-7e07536faede",
# META       "known_lakehouses": [
# META         {
# META           "id": "c232be5e-59da-4a9b-87df-e4b5307f5fce"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Laste opp portefølje dokumenter

# MARKDOWN ********************

# Notatboken henter ut prosjektinformasjon fra porteføljeskjemaet. Skjemaene lagres som Excel-filer i data lakehouse. Notisboken henter ut dataene fra Excel og lagrer dem i dataklasser.

# CELL ********************

%run "portfolio_data_schemas"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Last inn dataklasser **

# CELL ********************

%run "initial_report_wrangler"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Last inn funksjoner for å hente ut data og sette dem inn i dataklasser.**

# CELL ********************

mapping = {
    "Navn på tiltak": (3, 4),
    "Kontaktperson": (4, 4),
    "Hvilken fase er tiltaket i": (3, 9),
    "Oppstart/Planlagt oppstart": (4, 9),
    "Planlagt ferdig": (4, 11),
    "Avdeling": (5, 4),
    "Samarbeid med - internt": (7, 4),
    "Samarbeid med - eksternt": (8, 4),
    "Problemstilling": (12, 4),
    "Beskrivelse av tiltaket": (15, 4),
    "Risiko": (18, 4),
    "Hvilke kompetanser trenges for tiltaket?": (29, 6),
    "Er kompetanser tilgjengelige internt?": (31, 6),
    "Estimert antall månedsverk intern": (33, 7),
    "Estimert antall månedsverk ekstern": (34, 7),
    "Estimerte antall månedsverk eksterne som betales 2025": (36, 7),
    "Hvor sikkert er estimatene": (37, 7)
}
long_mapping = {**mapping, **{
    "Estimert budsjettbehov 2025 utover driftsrammen": (37, 7),
    "Hvilken måned vil første utbetaling skje (forfall)": (38, 7),
    "Hvor sikkert er estimatene": (39, 7)
}}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Mapping fil som peker på informasjon i excel mal. 

# CELL ********************

from notebookutils import mssparkutils
import pandas as pd
import io
import warnings

def read_in_xlsx_files(lakehouse_path):
    """
    Read Excel files from a Lakehouse directory in Azure Fabric.
    
    Args:
        lakehouse_path: Path to the directory containing Excel files in the Lakehouse
        
    Returns:
        A list of dictionaries with file names and dataframes
    """
    files = []
    
    # List files in the Lakehouse directory
    file_names_list = mssparkutils.fs.ls(lakehouse_path)
    
    # Filter Excel files
    excel_file_names = [file.name for file in file_names_list if file.name.endswith('.xlsx') or file.name.endswith('.xls')]
    filtered_file_names = filter_names(excel_file_names)

    for file_name in filtered_file_names:
        # Full path to the file
        file_path = f"{lakehouse_path}/{file_name}"
        
        # Read the Excel file
        # We need to use pandas to read Excel files
        binary_df = spark.read.format("binaryFile").load(file_path)
                 
        # Get binary content of the file
        binary_content = binary_df.first()["content"]
                    
        # Use pandas to read Excel from the binary content
        excel_data = io.BytesIO(binary_content)
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            df = pd.read_excel(excel_data)
            
        # Get base name without extension
        f_name = file_name.split(".")[0]
        
        files.append({"name": f_name, "file": df})
    
    return files

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Path to the directory in the Lakehouse
lakehouse_path = "Files/Porteføljeinitiativ"
    
# Read Excel files from the Lakehouse
portfolio_files = read_in_xlsx_files(lakehouse_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_projects = []
all_ressursbehovs = []
all_problemstillings = []
all_tiltaks = []
all_risikovurderings = []
all_fremskritts = []
    
for file in portfolio_files:
    if file["file"].shape[0] < 38:
        extracted_data = extract_data_to_json(file["file"], mapping)
    else:
        extracted_data = extract_data_to_json(file["file"], long_mapping)
        
    # Create the main project
    project = create_portfolio_project(extracted_data, file["name"])
    all_projects.append(project)
        
    # Create related entities
    ressursbehov = create_ressursbehov(extracted_data, project.project_id)
    all_ressursbehovs.append(ressursbehov)
        
    problemstilling = create_problemstilling(extracted_data, project.project_id)
    all_problemstillings.append(problemstilling)
        
    tiltak = create_tiltak(extracted_data, project.project_id)
    all_tiltaks.append(tiltak)
        
    risikovurdering = create_risikovurdering(extracted_data, project.project_id)
    all_risikovurderings.append(risikovurdering)
        
    fremskritt = create_fremskritt(extracted_data, project.project_id)
    all_fremskritts.append(fremskritt)

projects_df = pd.DataFrame([vars(p) for p in all_ressursbehovs])
##Neste skritt er å laste opp dataframes med pyspark

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
