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

# MARKDOWN ********************

# # 1️⃣ Usar %%sql para ejecutar SQL puro

# MARKDOWN ********************

# ## ✔️ Dimensión PGC (Plan General Contable)

# CELL ********************

# MAGIC %%sql
# MAGIC -- Dimensión PGC (Plan General Contable)
# MAGIC CREATE OR REPLACE TABLE lh_silver.dimPGC
# MAGIC SELECT  
# MAGIC         `Nombre Cuenta`  AS Nombre_Cuenta,
# MAGIC         `Nivel 1`        AS Nivel_1,
# MAGIC         `Nivel 2`        AS Nivel_2,
# MAGIC         `Nivel 3`        AS Nivel_3,
# MAGIC         `Nivel 4`        AS Nivel_4,
# MAGIC         Cuenta4D,
# MAGIC         OrdenNivel1,
# MAGIC         OrdenNivel2,
# MAGIC         OrdenNivel3,
# MAGIC         OrdenNivel4
# MAGIC     FROM lh_bronce_ERP_Microsoft.dbo_dimPGC
# MAGIC     WHERE Cuenta4D is NOT null ;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ✔️ Dimensión PyG (Pérdidas y Ganancias)

# MARKDOWN ********************


# CELL ********************

# MAGIC %%sql
# MAGIC -- Dimensión PyG (Pérdidas y Ganancias)
# MAGIC CREATE OR REPLACE TABLE lh_silver.dimPyG
# MAGIC     SELECT  
# MAGIC         `Nombre Cuenta`  AS Nombre_Cuenta,
# MAGIC         `Nivel 1`        AS Nivel_1,
# MAGIC         `Nivel 2`        AS Nivel_2,
# MAGIC         `Nivel 3`        AS Nivel_3,
# MAGIC         Cuenta4D,
# MAGIC         OrdenNivel1,
# MAGIC         OrdenNivel2,
# MAGIC         OrdenNivel3
# MAGIC     FROM lh_bronce_ERP_Microsoft.dbo_dimPyG
# MAGIC      WHERE Cuenta4D is NOT null ;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ✔️ Dimensión de Flujo de Caja (Cash Flow)

# CELL ********************

# MAGIC %%sql
# MAGIC --  Dimensión de Flujo de Caja (Cash Flow)
# MAGIC  CREATE OR REPLACE TABLE lh_silver.dbo_dimCF
# MAGIC     SELECT  
# MAGIC         `Nombre Cuenta`  AS Nombre_Cuenta,
# MAGIC         `Nivel 1`        AS Nivel_1,
# MAGIC         `Nivel 2`        AS Nivel_2,
# MAGIC         Cuenta4D,
# MAGIC         OrdenNivel1,
# MAGIC         OrdenNivel2
# MAGIC     FROM lh_bronce_ERP_Microsoft.dbo_dimCF
# MAGIC      WHERE Cuenta4D is NOT null ;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
