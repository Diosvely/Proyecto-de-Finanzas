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

# # Leer el archivo Csv `factDiario.csv` desde el Lakehouse (Bronce), transformarlo a DataFrame Spark y mostrar una vista previa.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType

# ------------------------
# Ruta del CSV en Lakehouse
# ------------------------
csv_path = "abfss://Financiero@onelake.dfs.fabric.microsoft.com/lh_crudo_ERPmicrosoft.Lakehouse/Files/factDiario.csv"

# ------------------------
# Lectura inicial con Spark
# - header=True: primera fila son nombres de columnas
# - inferSchema=True: Spark intenta detectar tipos automáticamente
# - sep=';' : separador de columnas (ajustar si es tab)
# ------------------------
df_spark = spark.read.option("header", True) \
                     .option("inferSchema", True) \
                     .option("sep", ";") \
                     .csv(csv_path)

# ------------------------
# Mostrar las primeras filas para validar la carga
# ------------------------
df_spark.show(5, truncate=False)

# ------------------------
# Mostrar esquema de columnas
# ------------------------
df_spark.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Crear una columna Combinada

# CELL ********************

from pyspark.sql import functions as F

# Agregar columna 'Combinada' uniendo 'G_L Account No_' y 'Description_' con "-"
df_spark = df_spark.withColumn(
    "Combinada",
    F.concat_ws("-", F.col("G_L Account No_"), F.col("Description_"))
)

# Mostrar algunas filas para validar
df_spark.select("G_L Account No_", "Description_", "Combinada").show(5, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #Crear  una columna personalizada para saber si un asiento es de regularización o no 

# CELL ********************

from pyspark.sql import functions as F

# Agregar columna 'Personalizado' según la lógica de M
df_spark = df_spark.withColumn(
    "Personalizado",
    F.when(F.col("Combinada") == "129000000-Asiento de regularización", 1)
     .when(~F.col("Combinada").contains("Asiento de regularización"), 1)
     .otherwise(0)
)

# Mostrar algunas filas para validar
df_spark.select("Combinada", "Personalizado").show(10, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Filtrar el dataframe quedandome solo con los personalizado = 1

# CELL ********************

# Filtrar el DataFrame para conservar solo filas donde Personalizado = 1
df_spark_filtrado = df_spark.filter(F.col("Personalizado") == 1)

# Mostrar algunas filas para validar
df_spark_filtrado.show(10, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Quitar Columnas que no se necesitan, dejo las que quiero 

# CELL ********************

# Seleccionar solo las columnas necesarias
df_spark_final = df_spark_filtrado.select(
    "G_L Account No_",
    "Posting Date",
    "Amount"
)

# Mostrar algunas filas para validar
df_spark_final.show(10, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Extraer los 4 primeros caracteres

# CELL ********************

from pyspark.sql import functions as F

# Mantener solo los primeros 4 caracteres de 'G_L Account No_'
df_spark_final = df_spark_final.withColumn(
    "G_L Account No_",
    F.col("G_L Account No_").substr(1, 4)  # indices empiezan en 1
)

# Mostrar algunas filas para validar
df_spark_final.show(10, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cambiar nombres a las Tablas 

# CELL ********************

# Renombrar columnas en df_spark_final
df_spark_final = df_spark_final.withColumnRenamed("G_L Account No_", "ID_cuenta") \
                               .withColumnRenamed("Posting Date", "Fecha") \
                               .withColumnRenamed("Amount", "Importe")

# Mostrar esquema y primeras filas para validar
df_spark_final.printSchema()
df_spark_final.show(10, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Cargar en la capa Silver los resultados 

# CELL ********************

import com.microsoft.spark.fabric                # Importa librerías Fabric para escribir en DW
from com.microsoft.spark.fabric.Constants import Constants  # Constantes para la integración

# Escribimos el DataFrame en el Data Warehouse, esquema Silver

# Sobrescribe la tabla si ya existe
df_spark_final.write \
    .mode("overwrite").synapsesql("DWH_FinancieroSilver.Silver.factDiario")  # Destino: DW, esquema Silver, tabla factDiario

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
