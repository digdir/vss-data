# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     },
# META     "environment": {}
# META   }
# META }

# CELL ********************

from dataclasses import dataclass
from datetime import datetime

@dataclass
class PortfolioProject:
    project_id: str
    navn: str
    oppstart: datetime
    avdeling: str
    kontaktpersoner: str
    strategisk_forankrings_ids : str # foreign key

@dataclass
class Ressursbehov:
    ressursbehov_id: str
    tidspunkt: datetime
    antall_mandsverk_intern: int
    antall_mandsverk_ekstern: int
    antall_mandsverk_ekstern_betalt: int
    risiko_av_estimat: str
    kompetanse_som_trengs: str
    kompetanse_tilgjengelig: str

@dataclass
class StrategiskForankring:
    strategisk_forankring_id: str
    base: str #Hoved_instruks, tildelingsbrev, digital strategi, ...
    kode: str #Hoved_instruks 3.1 
    beskrivelse: str #ref setningen

@dataclass
class Problemstilling:
    problem_stilling_id: str
    problem_stilling: str
    portofolio_project_id: str #Foreign key

@dataclass
class Tiltak:
    tiltak_id: str
    tiltak_beskrivelse: str
    problem_stilling_id: str # Foreign key
    portofolio_project_id: str #Foreign key

@dataclass
class Risikovurdering:
    risiko_vurdering_id: str
    portofolio_project_id: str #Foreign key
    vurdering: str

@dataclass 
class Fremskritt:
    fremskritt_id : str
    tidspunkt: datetime
    portofolio_project_id: str #Foreign key
    fase: str
    planlagt_ferdig: datetime
    ressursbehov_id: str #Foreign key
    risiko_vurdering_id: str #Foreign key


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from dataclasses import dataclass, asdict
from datetime import datetime
import pandas as pd

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Portfolio Management Schema Setup") \
    .getOrCreate()

# Function to convert dataclasses to Spark schemas
def create_schema_from_dataclass(dataclass_type):
    fields = []
    for field_name, field_type in dataclass_type.__annotations__.items():
        spark_type = None
        if field_type == int:
            spark_type = IntegerType()
        elif field_type == str:
            spark_type = StringType()
        elif field_type == datetime:
            spark_type = TimestampType()
        
        if spark_type:
            fields.append(StructField(field_name, spark_type, True))
    
    return StructType(fields)

# Create schema for each dataclass
portfolio_project_schema = create_schema_from_dataclass(PortfolioProject)
ressursbehov_schema = create_schema_from_dataclass(Ressursbehov)
strategisk_forankring_schema = create_schema_from_dataclass(StrategiskForankring)
problemstilling_schema = create_schema_from_dataclass(Problemstilling)
tiltak_schema = create_schema_from_dataclass(Tiltak)
risikovurdering_schema = create_schema_from_dataclass(Risikovurdering)
fremskritt_schema = create_schema_from_dataclass(Fremskritt)

# Define the path in the Data Lake
lake_path = 'abfss://7ef8faa8-ed08-4019-a01e-34fec5c0428e@onelake.dfs.fabric.microsoft.com/89f5499c-cbc0-4f83-8234-3211b382cf4d/Tables/'

# Create empty DataFrames with the schemas
portfolio_project_df = spark.createDataFrame([], portfolio_project_schema)
ressursbehov_df = spark.createDataFrame([], ressursbehov_schema)
strategisk_forankring_df = spark.createDataFrame([], strategisk_forankring_schema)
problemstilling_df = spark.createDataFrame([], problemstilling_schema)
tiltak_df = spark.createDataFrame([], tiltak_schema)
risikovurdering_df = spark.createDataFrame([], risikovurdering_schema)
fremskritt_df = spark.createDataFrame([], fremskritt_schema)

# Write the empty DataFrames to the Data Lake in Delta format
portfolio_project_df.write.format("delta").mode("overwrite").save(f"{lake_path}/portfolio_project")
ressursbehov_df.write.format("delta").mode("overwrite").save(f"{lake_path}/ressursbehov")
strategisk_forankring_df.write.format("delta").mode("overwrite").save(f"{lake_path}/strategisk_forankring")
problemstilling_df.write.format("delta").mode("overwrite").save(f"{lake_path}/problemstilling")
tiltak_df.write.format("delta").mode("overwrite").save(f"{lake_path}/tiltak")
risikovurdering_df.write.format("delta").mode("overwrite").save(f"{lake_path}/risikovurdering")
fremskritt_df.write.format("delta").mode("overwrite").save(f"{lake_path}/fremskritt")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils
def mount_datalake(data_lake_config, lake_name):
    """Make sure the lakehouse is mounted"""
    # Check if already mounted
    existing_mounts = [m.mountPoint for m in mssparkutils.fs.mounts()]
    
    # Mount if not already mounted
    if data_lake_config[lake_name] not in existing_mounts:
        mssparkutils.fs.mount(
            f"abfss://{data_lake_config[lake_name]['workspace_id']}@onelake.dfs.fabric.microsoft.com/{data_lake_config[lake_name]['lakehouse_id']}",
            data_lake_config[lake_name]["mount_point"]
        )
        print(f"Mounted lakehouse at {data_lake_config[lake_name]['mount_point']}")
    else:
        print(f"Lakehouse already mounted at {data_lake_config[lake_name]['mount_point']}")
    
    # Add the path to config
    data_lake_config[lake_name]["path"] = mssparkutils.fs.getMountPath(data_lake_config[lake_name]["mount_point"])
    return data_lake_config


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


import pandas as pd
# Load data into pandas DataFrame from f"{notebookutils.nbResPath}/builtin/data_lake_config.json"

config = pd.read_json(f"{notebookutils.nbResPath}/builtin/data_lake_config.json",typ="series")

data_lake_config = config["data_lakehouses"]
lake_name = "DataflowsStagingLakehouse"
path_to_lakehouse = mount_datalake(data_lake_config, lake_name)
print(path_to_lakehouse)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
