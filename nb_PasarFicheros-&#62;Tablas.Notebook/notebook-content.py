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

# # Manipular el fichero parquet para tranformarlo en tabla

# CELL ********************

 # 1) leer desde pandas el fichero parquet

import pandas as pd
df_cr_pandas = pd.read_parquet("/lakehouse/default/Files/Economia/Fact_asientocontable")
df_cr_pandas = df_cr_pandas.drop(columns=["System-Created Entry"])
df_cr_pandas = df_cr_pandas.drop(columns=["Global Dimension 1 Code"])
df_cr_pandas = df_cr_pandas.drop(columns=["Gen_ Posting Type"])
df_cr_pandas = df_cr_pandas.drop(columns=["Gen_ Bus_ Posting Group"])
df_cr_pandas = df_cr_pandas.drop(columns=["Business Unit Code"])
df_cr_pandas = df_cr_pandas.drop(columns=["Document Type"])
df_cr_pandas = df_cr_pandas.drop(columns=["Quantity"])


display(df_cr_pandas)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2) transformar de pandas a spark

from pyspark.sql import functions as F

df_cr = spark.createDataFrame(df_cr_pandas)
#display(df_cr)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
