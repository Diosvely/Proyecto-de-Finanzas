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

# # Leo la tabla que esta en el paso de crudo a silver las 3 tablas 

# CELL ********************

dimPGC = spark.sql(

"""
SELECT  `Nombre Cuenta` as Nombre_Cuenta ,
			`Nivel 1` as Nivel_1,
			`Nivel 2` as Nivel_2,
			`Nivel 3` as Nivel_3,
			`Nivel 4` as Nivel_4,
			Cuenta4D,
			OrdenNivel1,
			OrdenNivel2,
			OrdenNivel3,
			OrdenNivel4
FROM lh_bronce_ERP_Microsoft.dbo_dimPGC



"""

)

display(dimPGC)

##########

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dimPYG = spark.sql(

"""

SELECT  `Nombre Cuenta` as Nombre_Cuenta ,
			`Nivel 1` as Nivel_1,
			`Nivel 2` as Nivel_2,
			`Nivel 3` as Nivel_3,
			Cuenta4D,
			OrdenNivel1,
			OrdenNivel2,
			OrdenNivel3
FROM lh_bronce_ERP_Microsoft.dbo_dimPyG



"""

)

display(dimPYG)

##########

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dimCF = spark.sql(

"""
SELECT  `Nombre Cuenta` as Nombre_Cuenta ,
			`Nivel 1` as Nivel_1,
			`Nivel 2` as Nivel_2,
			Cuenta4D,
			OrdenNivel1,
			OrdenNivel2
FROM lh_bronce_ERP_Microsoft.dbo_dimCF


"""

)

display(dimCF)

##########

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Guardamos las 3 dimensiones en las tablas capa siver

dimPGC.write.mode("overwrite").saveAsTable("lh_Silver.dimPGC")
dimPYG.write.mode("overwrite").saveAsTable("lh_Silver.dimPYG")
dimCF.write.mode("overwrite").saveAsTable("lh_Silver.dimCF")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
