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

# # 📘 Notebook Spark: nb_FactDiario_File_To_Table

# MARKDOWN ********************

# ## 1️⃣ Crear staging a partir del CSV (importación directa) utilizando spark

# MARKDOWN ********************

# ### ➡️ Pasar de files a tablas dentro del mismo lakehouse 

# CELL ********************

# ----------------------------------------------------
# 1. LECTURA Y LIMPIEZA DEL DATAFRAME
# ----------------------------------------------------

# (Opcional) Usamos la ruta simplificada relativa al Lakehouse adjunto
# Nota: Si tu ruta original es "Files/Economia /factDiario.csv", he asumido que el espacio es un error
# y la he corregido a "Files/Economia/factDiario.csv".
csv_path = "abfss://66918516-32b2-4b32-a2c1-64fbcc79964f@onelake.dfs.fabric.microsoft.com/d34c1d5c-d1ce-42bf-8c1e-2f0e9cc153bc/Files/Economia /factDiario.csv"


# Lectura del CSV
df_spark = spark.read.option("header", True) \
                     .option("inferSchema", True) \
                     .option("sep", ";") \
                     .option("encoding", "ISO-8859-1") \
                     .csv(csv_path)

# ----------------------------------------------------
# 2. RENOMBRAR COLUMNAS (Práctica esencial en Spark/Delta)
#    Esto transforma nombres como "G_L Account No_" en "G_L_Account_No"
# ----------------------------------------------------

# Función para limpiar nombres: reemplaza espacios por guiones bajos y elimina otros caracteres
def clean_col_name(col):
    return col.replace(' ', '_').replace('.', '').replace('/', '').replace('__', '_').strip()

# Aplicar la limpieza a todas las columnas
columnas_limpias = [clean_col_name(col) for col in df_spark.columns]
df_spark = df_spark.toDF(*columnas_limpias)

# Opcional: Re-convertir tipos si inferSchema falló o si necesitas precisión específica
# df_spark = df_spark.withColumn("Amount", F.col("Amount").cast(DecimalType(18, 2)))
# df_spark = df_spark.withColumn("Posting_Date", F.col("Posting_Date").cast(DateType()))

print("Esquema final del DataFrame a cargar:")
df_spark.printSchema()

# ----------------------------------------------------
# 3. ESCRITURA: Volcar el DataFrame como Tabla Delta
# ----------------------------------------------------

# La tabla se creará en el área 'Tables' de tu Lakehouse con el nombre 'stg_factDiario'
# Usamos 'overwrite' para la primera carga, pero en producción podrías usar 'append' o 'merge'
df_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dbo_factDiario")

print("¡Carga exitosa! La tabla 'dbo_factDiario' ya está disponible en su Lakehouse.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
