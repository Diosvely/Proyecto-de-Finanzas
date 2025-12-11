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

# # 📘 Notebook SQL:   nb_Fact_Bronze_to_Silver

# MARKDOWN ********************

# # LLevar de la capa bonze to silver directo a través de SQL puro los cambios. 

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE lh_Silver.fact_Diario AS
# MAGIC  WITH Base AS (
# MAGIC      -- Limpieza inicial del CSV
# MAGIC      SELECT
# MAGIC          LEFT(G_L_Account_No_, 4)        AS ID_cuenta,
# MAGIC          to_date(Posting_Date, 'dd/MM/yyyy') AS Fecha,
# MAGIC          TRY_CAST(REPLACE(Amount, ',', '.') AS FLOAT) AS Importe,
# MAGIC          Debit_Amount AS Debito,
# MAGIC          Credit_Amount AS Credito,
# MAGIC          CONCAT(G_L_Account_No_, '-', Description_) AS Combinada
# MAGIC      FROM lh_bronce_ERP_Microsoft.dbo_factdiario 
# MAGIC  ),
# MAGIC  Marcado AS (
# MAGIC      -- Lógica de Personalizado (marcar regularizaciones)
# MAGIC     SELECT *,
# MAGIC          CASE
# MAGIC              WHEN Combinada = '129000000-Asiento de regularización' THEN 1
# MAGIC             WHEN Combinada NOT LIKE '%Asiento de regularización%' THEN 1
# MAGIC              ELSE 0
# MAGIC         END AS Personalizado
# MAGIC      FROM Base
# MAGIC  ),
# MAGIC  Filtrado AS (
# MAGIC     -- Filtrar filas válidas
# MAGIC      SELECT *
# MAGIC      FROM Marcado
# MAGIC      WHERE Personalizado = 1
# MAGIC  )
# MAGIC  -- Resultado final
# MAGIC  SELECT
# MAGIC      ID_cuenta,
# MAGIC     Fecha,
# MAGIC      Importe,
# MAGIC      Debito,
# MAGIC      Credito
# MAGIC  FROM Filtrado;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
