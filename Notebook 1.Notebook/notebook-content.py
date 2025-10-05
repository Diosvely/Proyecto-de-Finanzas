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

# # Leer el archivo Excel `dimFilasPYG.xlsx` desde el Lakehouse (Bronce), transformarlo a DataFrame Spark y mostrar una vista previa.

# CELL ********************

import pandas as pd  # Librería para manejar Excel

# Ruta del archivo Excel en Lakehouse
excel_path = "abfss://Financiero@onelake.dfs.fabric.microsoft.com/lh_crudo_ERPmicrosoft.Lakehouse/Files/dimFilasPYG.xlsx"

# Leemos la hoja 'dimFilasPYG' en un DataFrame de pandas
df_pandas = pd.read_excel(excel_path, sheet_name="dimFilasPYG")

# Convertimos el DataFrame de pandas a Spark
df_spark = spark.createDataFrame(df_pandas)

# Mostramos las primeras filas para validar la carga
df_spark.show()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Guardar datos de la tabla dimFilasPYG en el warehouse schema silver

# CELL ********************

import com.microsoft.spark.fabric                # Importa librerías Fabric para escribir en DW
from com.microsoft.spark.fabric.Constants import Constants  # Constantes para la integración

# Escribimos el DataFrame en el Data Warehouse, esquema Silver
# Sobrescribe la tabla si ya existe
df_spark.write \
    .mode("overwrite").synapsesql("DWH_FinancieroSilver.Silver.dimFilasPYG")  # Destino: DW, esquema Silver, tabla dimFilasPYG


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
