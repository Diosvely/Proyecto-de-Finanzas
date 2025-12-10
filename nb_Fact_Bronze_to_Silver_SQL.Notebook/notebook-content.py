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

# # 📘 Notebook SQL: nb_Fact_Bronze_to_Silver_SQL

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

# MARKDOWN ********************

# # 2️⃣ LLevar de la capa bonze to silver directo a traves de SQL puro los cambios 

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
