# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e8f51854-cd89-40a2-b9e6-778f0391713c",
# META       "default_lakehouse_name": "lh_Silver",
# META       "default_lakehouse_workspace_id": "66918516-32b2-4b32-a2c1-64fbcc79964f",
# META       "known_lakehouses": [
# META         {
# META           "id": "e8f51854-cd89-40a2-b9e6-778f0391713c"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook: Pasar tablas Silver → Gold cuando esta en un mismo datawarehause

# CELL ********************

# Notebook: Pasar tablas Silver → Gold
# --------------------------------------

# 1️⃣ Importar librerías
from pyspark.sql import functions as F
import com.microsoft.spark.fabric

# 2️⃣ Definir rutas de tablas Silver y destino Gold
silver_schema = "DWH_FinancieroSilver.Silver"
gold_schema   = "DWH_FinancieroSilver.Gold"

tables = {
    "dimFilasPYG": {"silver": f"{silver_schema}.dimFilasPYG", "Gold": f"{gold_schema}.dimFilasPYG"},
    "dimPGC":     {"silver": f"{silver_schema}.dimPGC",     "Gold": f"{gold_schema}.dimPGC"},
    "fact_Diario": {"silver": f"{silver_schema}.fact_Diario", "Gold": f"{gold_schema}.fact_Diario"}
}

# 3️⃣ Función para leer tabla Silver, validar tipos y escribir en Gold
def silver_to_gold(silver_name, gold_name):
    print(f"\nProcesando tabla: {silver_name}")
    
    # Leer tabla Silver
    df = spark.read.synapsesql(silver_name)
    
    # Mostrar esquema original
    print("Esquema original:")
    df.printSchema()
    
    # Validaciones y conversiones según tabla
    if silver_name in ["dimFilasPYG", "dimPGC"]:
        # En estas tablas todas las columnas se dejan como string por defecto
        for col_name, col_type in df.dtypes:
            df = df.withColumn(col_name, F.col(col_name).cast("string"))
    
    elif silver_name == "fact_Diario":
        # Convertir columnas críticas
        df = df.withColumn("Fecha", F.to_date(F.col("Fecha"), "dd/MM/yyyy"))  # Fecha a tipo date
        df = df.withColumn("Importe", F.col("Importe").cast("double"))        # Importe a double
        df = df.withColumn("ID_cuenta", F.col("ID_cuenta").cast("string"))    # ID_cuenta a string
    
    # Mostrar resumen de filas y nulos
    df.select([F.count("*").alias("Total")] + [F.count(F.when(F.col(c).isNull(), c)).alias(c+"_Nulos") for c in df.columns]).show()
    
    # Escribir tabla validada en Gold
    df.write.mode("overwrite").synapsesql(gold_name)
    print(f"✅ Tabla {gold_name} escrita en Gold correctamente.")
    
    return df

# 4️⃣ Ejecutar la función para cada tabla
for t_name, paths in tables.items():
    silver_to_gold(paths["silver"], paths["Gold"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Notebook: Pasar tablas desde lh_silver → dwh_gold_dimensional
# # -------------------------------------------------------------

# CELL ********************

# Notebook: Pasar tablas desde lh_silver → dwh_gold_dimensional
# -------------------------------------------------------------

from pyspark.sql import functions as F
import com.microsoft.spark.fabric

# 1️⃣ Definir origen (Lakehouse) y destino (Warehouse)
lakehouse_name = "lh_silver"
warehouse_name = "dwh_Gold_dimensional&IA"
schema_destino = "Gold_dimensional"   # Puedes cambiarlo si quieres BI / IA

# 2️⃣ Tablas a procesar
tables = {
    "dimpyg":      {"silver": f"{lakehouse_name}.dimpyg",      "gold": f"{warehouse_name}.{schema_destino}.dimpyg"},
    "dimcf":       {"silver": f"{lakehouse_name}.dimcf",       "gold": f"{warehouse_name}.{schema_destino}.dimcf"},
    "dimpgc":      {"silver": f"{lakehouse_name}.dimpgc",      "gold": f"{warehouse_name}.{schema_destino}.dimpgc"},
    "fact_diario": {"silver": f"{lakehouse_name}.fact_diario", "gold": f"{warehouse_name}.{schema_destino}.fact_diario"}
}

# 3️⃣ Función para mover tabla Lakehouse → Warehouse
def lake_to_dwh(silver_table, gold_table):
    print(f"\n📌 Procesando tabla: {silver_table}")

    # Leer tabla desde el Lakehouse
    df = spark.read.table(silver_table)

    print("Esquema original:")
    df.printSchema()

    # -----------------------------------------------------------------------
    # 🔥 CONVERSIONES ESPECÍFICAS PARA fact_diario
    # -----------------------------------------------------------------------
    if "fact_diario" in silver_table.lower():

        # Fecha → DATE
        if "Fecha" in df.columns:
            df = df.withColumn("Fecha", F.to_date(F.col("Fecha")))

        # Importe → double
        if "Importe" in df.columns:
            df = df.withColumn("Importe", F.col("Importe").cast("double"))

        # ID_cuenta → string
        if "ID_cuenta" in df.columns:
            df = df.withColumn("ID_cuenta", F.col("ID_cuenta").cast("string"))

        # Debito → double
        if "Debito" in df.columns:
            df = df.withColumn("Debito", F.col("Debito").cast("double"))

        # Credito → double
        if "Credito" in df.columns:
            df = df.withColumn("Credito", F.col("Credito").cast("double"))

    # -----------------------------------------------------------------------
    # ✔ DIMENSIONES (dimpyg, dimcf, dimpgc) → todo como string
    # -----------------------------------------------------------------------
    if "dim" in silver_table.lower() and "fact" not in silver_table.lower():
        for col_name, col_type in df.dtypes:
            df = df.withColumn(col_name, F.col(col_name).cast("string"))

    # -----------------------------------------------------------------------
    # Resumen de nulos
    # -----------------------------------------------------------------------
    df.select(
        [F.count("*").alias("Total")] +
        [F.count(F.when(F.col(c).isNull(), c)).alias(c + "_Nulos") for c in df.columns]
    ).show()

    # -----------------------------------------------------------------------
    # Escribir en el Warehouse
    # -----------------------------------------------------------------------
    print(f"➡ Escribiendo en: {gold_table}")
    df.write.mode("overwrite").synapsesql(gold_table)

    print(f"✅ Tabla {gold_table} copiada correctamente.\n")
    return df


# 4️⃣ Ejecutar proceso para todas las tablas
for t_name, paths in tables.items():
    lake_to_dwh(paths["silver"], paths["gold"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
