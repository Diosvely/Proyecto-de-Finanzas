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

# # Creación de una tabla calendario en Spark

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# -------------------------------------------------
# 0️⃣ Configuración regional en español (intento 1)
# -------------------------------------------------
spark.conf.set("spark.sql.session.timeZone", "Europe/Madrid")
spark.conf.set("spark.sql.locale", "es_ES")

# -------------------------------------------------
# 1️⃣ Leer la tabla fact_Diario desde Silver
# -------------------------------------------------

#fact_df = spark.read.synapsesql("WarehouseName.Schema.Table") \                                para leerlo desde un Warehouse
 #                 .withColumn("Fecha", F.to_date(F.col("Fecha"), "dd/MM/yyyy"))

fact_df = spark.read.table("lh_Silver.fact_diario") \
                    .withColumn("Fecha", F.to_date(F.col("Fecha"), "dd/MM/yyyy"))

# -------------------------------------------------
# 2️⃣ Rango de fechas
# -------------------------------------------------
min_date, max_date = fact_df.agg(
    F.min("Fecha").alias("min_date"),
    F.max("Fecha").alias("max_date")
).first()

# -------------------------------------------------
# 3️⃣ Generar calendario
# -------------------------------------------------
cal_df = spark.createDataFrame([(min_date, max_date)], ["start", "end"]) \
    .select(F.explode(F.sequence(F.col("start"), F.col("end"))).alias("Fecha"))

# -------------------------------------------------
# 4️⃣ Diccionarios para meses y días (por si locale falla)
# -------------------------------------------------
meses_dict = {
    "January":"Enero","February":"Febrero","March":"Marzo","April":"Abril",
    "May":"Mayo","June":"Junio","July":"Julio","August":"Agosto",
    "September":"Septiembre","October":"Octubre","November":"Noviembre","December":"Diciembre"
}

dias_dict = {
    "Monday":"Lunes","Tuesday":"Martes","Wednesday":"Miércoles",
    "Thursday":"Jueves","Friday":"Viernes","Saturday":"Sábado","Sunday":"Domingo"
}

trad_mes = F.udf(lambda x: meses_dict.get(x, x), StringType())
trad_dia = F.udf(lambda x: dias_dict.get(x, x), StringType())

# -------------------------------------------------
# 5️⃣ Columnas del calendario (ESPAÑOL)
# -------------------------------------------------
cal_df = cal_df \
    .withColumn("Año", F.year("Fecha")) \
    .withColumn("Trimestre", F.quarter("Fecha")) \
    .withColumn("NumMes", F.month("Fecha")) \
    .withColumn("Mes_temp", F.date_format("Fecha", "MMMM")) \
    .withColumn("Mes", trad_mes(F.col("Mes_temp"))) \
    .withColumn("MesCorto", F.substring("Mes", 1, 3)) \
    .withColumn("Día", F.dayofmonth("Fecha")) \
    .withColumn("Dia_temp", F.date_format("Fecha", "EEEE")) \
    .withColumn("NombreDíaSemana", trad_dia(F.col("Dia_temp"))) \
    .withColumn("DíaSemana", F.dayofweek("Fecha")) \
    .withColumn("PrimerDíaMes", F.trunc("Fecha", "MM")) \
    .withColumn("ÚltimoDíaMes", F.last_day("Fecha")) \
    .withColumn("PrimerDíaTrimestre", F.expr("add_months(trunc(Fecha,'Q'),0)")) \
    .withColumn("ÚltimoDíaTrimestre", F.expr("add_months(trunc(Fecha,'Q'),3)-interval 1 day")) \
    .withColumn("PrimerDíaAño", F.trunc("Fecha", "YYYY")) \
    .withColumn("ÚltimoDíaAño", F.expr("add_months(trunc(Fecha,'YYYY'),12)-interval 1 day")) \
    .withColumn("Laborable_FinSemana", 
                F.when(F.dayofweek("Fecha").between(2,6),"Laborable").otherwise("Festivo")) \
    .withColumn("CuentaLaborable", F.when(F.dayofweek("Fecha").between(2,6),1).otherwise(0)) \
    .withColumn("CuentaFinDeSemana", F.when(F.dayofweek("Fecha").between(2,6),0).otherwise(1)) \
    .withColumn("Estacion", 
                F.when(F.date_format("Fecha", "MMdd") < "0320","Invierno")
                 .when(F.date_format("Fecha", "MMdd") < "0621","Primavera")
                 .when(F.date_format("Fecha", "MMdd") < "0921","Verano")
                 .when(F.date_format("Fecha", "MMdd") < "1221","Otoño")
                 .otherwise("Invierno")) \
    .withColumn("Semana", F.concat(F.lit("Sem "), F.weekofyear("Fecha"))) \
    .withColumn("OrdenSemana", F.weekofyear("Fecha")) \
    .withColumn("AñoSemana", F.concat(F.col("Año"), F.lit("-"), F.col("Semana"))) \
    .withColumn("OrdenAñoSemana", F.col("Año")*100 + F.col("OrdenSemana")) \
    .drop("Mes_temp", "Dia_temp")  # limpieza

# Mostrar
cal_df.show(30, truncate=False)






# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Guardar la tabla calendario en Gold

# CELL ********************

# -------------------------------------------------
#  Guardar tabla en Silver
# -------------------------------------------------
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

cal_df.write.mode("overwrite") \
       .synapsesql("dwh_Gold_dimensional&IA.Gold_dimensional.dim_Calendario")

print("✅ Tabla dim_Calendario creada correctamente en Gold")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
