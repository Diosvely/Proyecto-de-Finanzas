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

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pypdf import PdfReader, PdfWriter

# Ruta al PDF (corrige si es necesario)
input_pdf = "/lakehouse/default/Files/Economia/PGC 2021 ESPAÑA.pdf"

# Rango de páginas (1-indexed)
start_page = 197
end_page = 230

# Archivo de salida
output_pdf = "/lakehouse/default/Files/Economia/PGC_197_230.pdf"

# Leer PDF original
reader = PdfReader(input_pdf)
writer = PdfWriter()

# Extraer páginas (pypdf usa índice base 0)
for page_num in range(start_page - 1, end_page):
    writer.add_page(reader.pages[page_num])

# Guardar resultado
with open(output_pdf, "wb") as f:
    writer.write(f)

print(f"PDF extraído correctamente en: {output_pdf}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
