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

# # ===================
# # Notebook: nb_Dimensiones_Bronze_to_Silver
# ### Objetivo:
# ###   - Leer tablas dimensionales desde capa Bronze (Lakehouse lh_bronce_ERP_Microsoft)
# ###   - Estandarizar nombres de columnas
# ###   - Escribir las tablas limpias en la capa Silver (Lakehouse lh_silver)
# # ====================

# MARKDOWN ********************

# ## 1. Cargar tablas dimension en Bronze

# CELL ********************

# Dimensión PGC (Plan General Contable)
dimPGC = spark.sql("""
    SELECT  
        `Nombre Cuenta`  AS Nombre_Cuenta,
        `Nivel 1`        AS Nivel_1,
        `Nivel 2`        AS Nivel_2,
        `Nivel 3`        AS Nivel_3,
        `Nivel 4`        AS Nivel_4,
        Cuenta4D,
        OrdenNivel1,
        OrdenNivel2,
        OrdenNivel3,
        OrdenNivel4
    FROM lh_bronce_ERP_Microsoft.dbo_dimPGC
""")

display(dimPGC)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Dimensión PyG (Pérdidas y Ganancias)
dimPYG = spark.sql("""
    SELECT  
        `Nombre Cuenta`  AS Nombre_Cuenta,
        `Nivel 1`        AS Nivel_1,
        `Nivel 2`        AS Nivel_2,
        `Nivel 3`        AS Nivel_3,
        Cuenta4D,
        OrdenNivel1,
        OrdenNivel2,
        OrdenNivel3
    FROM lh_bronce_ERP_Microsoft.dbo_dimPyG
""")

display(dimPYG)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Dimensión de Flujo de Caja (Cash Flow)
dimCF = spark.sql("""
    SELECT  
        `Nombre Cuenta`  AS Nombre_Cuenta,
        `Nivel 1`        AS Nivel_1,
        `Nivel 2`        AS Nivel_2,
        Cuenta4D,
        OrdenNivel1,
        OrdenNivel2
    FROM lh_bronce_ERP_Microsoft.dbo_dimCF
""")

display(dimCF)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Guardar dimensiones en capa Silver
# ### ---------------------------------------------------------------


# CELL ********************

# Guardamos las 3 dimensiones en las tablas capa siver
# Modo "overwrite": se reemplaza la tabla completa en cada ejecución
# saveAsTable: crea tabla administrada en el Lakehouse Silver

dimPGC.write.mode("overwrite").saveAsTable("lh_Silver.dimPGC")
dimPYG.write.mode("overwrite").saveAsTable("lh_Silver.dimPYG")
dimCF.write.mode("overwrite").saveAsTable("lh_Silver.dimCF")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
