# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ddfa4daa-a232-49d3-9de0-5ab33ad9f550",
# META       "default_lakehouse_name": "lh_crudo_ERPmicrosoft",
# META       "default_lakehouse_workspace_id": "d2933686-1449-435d-bcfc-e34b63913adb",
# META       "known_lakehouses": [
# META         {
# META           "id": "ddfa4daa-a232-49d3-9de0-5ab33ad9f550"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Leer Datos del archivo dimFilasPYG y cargarlos en un dateframe

# CELL ********************

import pandas as pd

excel_path = "abfss://Financiero@onelake.dfs.fabric.microsoft.com/lh_crudo_ERPmicrosoft.Lakehouse/Files/dimFilasPYG.xlsx"


df_pandas = pd.read_excel(excel_path, sheet_name="dimFilasPYG")

df_spark = spark.createDataFrame(df_pandas)
df_spark.show()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Guardar datos de la tabla dimFilasPYG en el warehouse schema silver

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants  

df_spark.write \
    .mode("overwrite") \
    .synapsesql("DWH_FinancieroSilver.Silver.dimFilasPYG")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

