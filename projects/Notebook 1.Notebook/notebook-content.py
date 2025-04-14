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

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

df = spark.sql("SELECT * FROM ProjectPortfolio.Portfolio_data LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils
mssparkutils.fs.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.ls("abfss://4049da66-dd02-4708-a98e-7e07536faede@onelake.dfs.fabric.microsoft.com/c232be5e-59da-4a9b-87df-e4b5307f5fce/Tables/List_portfolio") # works with the default lakehouse files using relative path 
# mssparkutils.fs.ls("abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<path>")  # based on ABFS file system 
# mssparkutils.fs.ls("file:/tmp")  # based on local file system of driver node 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.parquet("abfss://4049da66-dd02-4708-a98e-7e07536faede@onelake.dfs.fabric.microsoft.com/c232be5e-59da-4a9b-87df-e4b5307f5fce/Tables/List_portfolio")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
