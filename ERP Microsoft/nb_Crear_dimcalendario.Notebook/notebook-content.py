# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import com.microsoft.spark.fabric

# 1️⃣ Leer la tabla fact_Diario desde Gold
fact_df = spark.read.synapsesql("DWH_FinancieroSilver.Gold.fact_Diario") \
                  .withColumn("Fecha", F.to_date(F.col("Fecha"), "dd/MM/yyyy"))

# -------------------------------------------------
# 2️⃣ Obtener rango de fechas
# -------------------------------------------------
min_date, max_date = fact_df.agg(
    F.min("Fecha").alias("min_date"),
    F.max("Fecha").alias("max_date")
).first()

# -------------------------------------------------
# 3️⃣ Generar calendario usando sequence y explode
# -------------------------------------------------
cal_df = spark.createDataFrame([(min_date, max_date)], ["start", "end"]) \
    .select(F.explode(F.sequence(F.col("start"), F.col("end"))).alias("Fecha"))

# -------------------------------------------------
# 4️⃣ Agregar columnas útiles
# -------------------------------------------------
cal_df = cal_df \
    .withColumn("Año", F.year("Fecha")) \
    .withColumn("Trimestre", F.quarter("Fecha")) \
    .withColumn("NumMes", F.month("Fecha")) \
    .withColumn("Mes", F.date_format("Fecha", "MMMM")) \
    .withColumn("MesCorto", F.date_format("Fecha", "MMM")) \
    .withColumn("Día", F.dayofmonth("Fecha")) \
    .withColumn("DíaSemana", F.dayofweek("Fecha")) \
    .withColumn("NombreDíaSemana", F.date_format("Fecha", "EEEE")) \
    .withColumn("PrimerDíaMes", F.trunc("Fecha", "MM")) \
    .withColumn("ÚltimoDíaMes", F.last_day("Fecha")) \
    .withColumn("PrimerDíaTrimestre", F.expr("add_months(trunc(Fecha,'Q'),0)")) \
    .withColumn("ÚltimoDíaTrimestre", F.expr("add_months(trunc(Fecha,'Q'),3)-interval 1 day")) \
    .withColumn("PrimerDíaAño", F.trunc("Fecha", "YYYY")) \
    .withColumn("ÚltimoDíaAño", F.expr("add_months(trunc(Fecha,'YYYY'),12)-interval 1 day")) \
    .withColumn("Laborable_FinSemana", F.when(F.dayofweek("Fecha").between(2,6),"Laborable").otherwise("Festivo")) \
    .withColumn("CuentaLaborable", F.when(F.dayofweek("Fecha").between(2,6),1).otherwise(0)) \
    .withColumn("CuentaFinDeSemana", F.when(F.dayofweek("Fecha").between(2,6),0).otherwise(1)) \
    .withColumn("Estacion", 
                F.when(F.date_format("Fecha", "MMdd") < "0320","INVIERNO")
                 .when(F.date_format("Fecha", "MMdd") < "0621","PRIMAVERA")
                 .when(F.date_format("Fecha", "MMdd") < "0921","VERANO")
                 .when(F.date_format("Fecha", "MMdd") < "1221","OTOÑO")
                 .otherwise("INVIERNO")) \
    .withColumn("Semana", F.concat(F.lit("Sem "), F.weekofyear("Fecha"))) \
    .withColumn("OrdenSemana", F.weekofyear("Fecha")) \
    .withColumn("AñoSemana", F.concat(F.col("Año"), F.lit("-"), F.col("Semana"))) \
    .withColumn("OrdenAñoSemana", F.col("Año")*100 + F.col("OrdenSemana"))

cal_df.show()




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Guardar la tabla calendario en Gold

# CELL ********************

# -------------------------------------------------
#  Guardar tabla en Gold
# -------------------------------------------------
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

cal_df.write.mode("overwrite") \
       .synapsesql("DWH_FinancieroSilver.Gold.dim_Calendario")

print("✅ Tabla dim_Calendario creada correctamente en Gold")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
