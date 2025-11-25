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
# META         },
# META         {
# META           "id": "d34c1d5c-d1ce-42bf-8c1e-2f0e9cc153bc"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Leemos los ficheros spark que estan en la carpeta humanresuorces y los materializamos en dataframes(materializar parquet en tablas)

# CELL ********************

# Codigo para leer una tabla para saber que tiene 
df_revisar = spark.read.table("lh_bronce_ERP_Microsoft.factDiario")
df_revisar   # para saber el tipo de datos 
display(df_revisar) # para ver los datos



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Guardar datos de la tabla dimPGC en el warehouse schema silver

# CELL ********************

import com.microsoft.spark.fabric                # Importa librerías Fabric para escribir en DW
from com.microsoft.spark.fabric.Constants import Constants  # Constantes para la integración

# Escribimos el DataFrame en el Data Warehouse, esquema Silve

# Sobrescribe la tabla si ya existe
df_spark.write \
    .mode("overwrite").synapsesql("DWH_FinancieroSilver.Silver.dimPGC")  # Destino: DW, esquema Silver, tabla dimPGC


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
