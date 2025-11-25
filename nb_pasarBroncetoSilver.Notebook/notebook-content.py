# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d34c1d5c-d1ce-42bf-8c1e-2f0e9cc153bc",
# META       "default_lakehouse_name": "lh_bronce_ERP_Microsoft",
# META       "default_lakehouse_workspace_id": "66918516-32b2-4b32-a2c1-64fbcc79964f",
# META       "known_lakehouses": [
# META         {
# META           "id": "d34c1d5c-d1ce-42bf-8c1e-2f0e9cc153bc"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Leer el fichero Parquet desde Lakehouse
df_spark = spark.read.parquet("/lakehouse/default/Files/Economia/FactDiario")



# Mostrar filas
df_spark.show(5)

# Mostrar el esquema
df_spark.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils

mssparkutils.fs.ls("/lakehouse/default/Files")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
