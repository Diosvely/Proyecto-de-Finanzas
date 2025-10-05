# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

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
