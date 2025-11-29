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

# # 📘 Notebook: nb_Fact_Bronze_to_Silver_Spark

# MARKDOWN ********************

# ### 1️⃣ Leer el fichero factDiario.csv desde la capa Bronze del Lakehouse y obtener un DataFrame inicial.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType

# ------------------------
# Ruta del CSV en Lakehouse
# ------------------------
csv_path = "abfss://66918516-32b2-4b32-a2c1-64fbcc79964f@onelake.dfs.fabric.microsoft.com/d34c1d5c-d1ce-42bf-8c1e-2f0e9cc153bc/Files/Economia /factDiario.csv"

# ------------------------
# Lectura inicial con Spark
# - header=True: primera fila son nombres de columnas
# - inferSchema=True: Spark intenta detectar tipos automáticamente
# - sep=';' : separador de columnas (ajustar si es tab)
# ------------------------
df_spark = spark.read.option("header", True) \
                     .option("inferSchema", True) \
                     .option("sep", ";") \
                     .option("encoding", "ISO-8859-1") \
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

# ## 2️⃣ Convertir Amount en número lo interpretaba como texto string

# CELL ********************

from pyspark.sql import functions as F

# Convertir "Amount" de string con coma decimal a double
df_spark = df_spark.withColumn(
    "Amount",
    F.regexp_replace("Amount", ",", ".").cast("double")
)

# Verificar esquema
df_spark.printSchema()

# Verificar primeras filas
df_spark.select("Amount").show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##  3️⃣ Comprobación de suma total (control de calidad)

# CELL ********************

# Calcular la suma total de la columna Importe
suma_importe = df_spark.agg(F.sum("Amount").alias("Total_Importe")).collect()[0]["Total_Importe"]

print(f"Suma total de Importe: {suma_importe}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##  4️⃣ Código para inspeccionar cada columna y mostrar los caracteres no ASCII que contiene

# CELL ********************

# este codigo me sirve para saber como cogio los caracteres especiales o sea tildes y ñ
from pyspark.sql import functions as F
import string

# Función auxiliar para detectar caracteres no ASCII
def non_ascii_chars(col_name):
    return F.udf(lambda x: ''.join([c for c in str(x) if ord(c) > 127]) if x is not None else '', "string")

# Iterar sobre todas las columnas
for col in df_spark.columns:
    print(f"\n--- Columna: {col} ---")
    
    # Crear columna temporal con los caracteres no ASCII
    df_check = df_spark.withColumn("NonASCII", non_ascii_chars(col)(F.col(col)))
    
    # Mostrar las filas que tienen caracteres no ASCII
    df_check.filter(F.col("NonASCII") != "").select(col, "NonASCII").show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5️⃣ Crear una columna Combinada

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

# ## 6️⃣ Crear una columna personalizada para saber si un asiento es de regularización o no

# CELL ********************

from pyspark.sql import functions as F

# Crear columna 'Personalizado' replicando la lógica exacta de M
df_spark = df_spark.withColumn(
    "Personalizado",
    F.when(F.col("Combinada") == "129000000-Asiento de regularización", 1)  # Exact match → 1
     .when(~F.col("Combinada").contains("Asiento de regularización"), 1)     # No contiene → 1
     .otherwise(0)                                                           # Todo lo demás → 0
)

# Mostrar algunas filas para validar
df_spark.select("Combinada", "Personalizado").show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7️⃣ Filtrar el dataframe quedandome solo con los personalizado = 1

# MARKDOWN ********************

# # 

# CELL ********************

from pyspark.sql import functions as F

# -------------------------------------------------
# Contar los valores de la columna 'Personalizado'
# -------------------------------------------------
# groupBy("Personalizado") -> Agrupa el DataFrame por la columna Personalizado (0 y 1)
# agg(F.count("*").alias("Cantidad")) -> Cuenta cuántas filas hay en cada grupo y asigna el nombre 'Cantidad'
conteo_personalizado = df_spark.groupBy("Personalizado") \
                               .agg(F.count("*").alias("Cantidad"))

# Mostrar el resultado en consola
# La tabla mostrará cuántos registros tienen Personalizado = 0 y cuántos tienen Personalizado = 1
conteo_personalizado.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# ## 8️⃣ Comprobar que la Suma despues de filtrar esta OK

# CELL ********************

# Calcular la suma total de la columna Importe
suma_importe = df_spark_filtrado.agg(F.sum("Amount").alias("Total_Importe")).collect()[0]["Total_Importe"]

print(f"Suma total de Importe: {suma_importe}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9️⃣ Quitar Columnas que no se necesitan, dejo las que quiero

# CELL ********************

# Seleccionar solo las columnas necesarias
df_spark_final = df_spark_filtrado.select(
    "G_L Account No_",
    "Posting Date",
    "Amount",
    "Debit Amount",
    "Credit Amount"
)

# Mostrar algunas filas para validar
df_spark_final.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 🔟 Extraer los 4 primeros caracteres para crear un columna para relacionar con las dimensiones

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

# ## 1️⃣1️⃣ Cambiar nombres a las Tablas

# CELL ********************

# Renombrar columnas en df_spark_final
df_spark_final = df_spark_final.withColumnRenamed("G_L Account No_", "ID_cuenta") \
                               .withColumnRenamed("Posting Date", "Fecha") \
                               .withColumnRenamed("Amount", "Importe")\
                               .withColumnRenamed("Debit Amount", "Debito")\
                               .withColumnRenamed("Credit Amount", "Credito")

# Mostrar esquema y primeras filas para validar
df_spark_final.printSchema()
df_spark_final.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1️⃣ 2️⃣ Comprobar que el importe es el correcto antes de llevarlo a la capa plata

# CELL ********************

# Calcular la suma total de la columna Importe
suma_importe = df_spark_filtrado.agg(F.sum("Amount").alias("Total_Importe")).collect()[0]["Total_Importe"]

print(f"Suma total de Importe: {suma_importe}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##  1️⃣ 3️⃣  LLevar a la capa Silver los resultados de factdiario

# CELL ********************

df_spark_final.write.mode("overwrite").saveAsTable("lh_Silver.fact_Diario")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1️⃣ 4️⃣  Esto es opcionar por si quiero copiar en un datawarehause en ves de un lakehause

# CELL ********************

import com.microsoft.spark.fabric                # Importa librerías Fabric para escribir en DW
from com.microsoft.spark.fabric.Constants import Constants  # Constantes para la integración

# Escribimos el DataFrame en el Data Warehouse, esquema Silver

# Sobrescribe la tabla si ya existe
df_spark_final.write \
    .mode("overwrite").synapsesql("lh_Silver.fact_Diario")  # Destino: DW, esquema Silver, tabla factDiario

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
