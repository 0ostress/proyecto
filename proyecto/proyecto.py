import pandas as pd
from sqlalchemy import create_engine

# =========================
# CONEXIÓN A SQL SERVER
# =========================
engine = create_engine(
    "mssql+pyodbc://@localhost/pruebin?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)

print("Conexión a la base de datos establecida")

# =========================
# EXTRACCIÓN (CSV)
# =========================
clientes = pd.read_csv("data/clientes.csv")
productos = pd.read_csv("data/productos.csv")
fuentes = pd.read_csv("data/fuentes.csv")
opiniones = pd.read_csv("data/opiniones.csv")

print("Archivos CSV cargados correctamente")

# =========================
# TRANSFORMACIÓN
# =========================
# Eliminar duplicados
clientes.drop_duplicates(inplace=True)
productos.drop_duplicates(inplace=True)
fuentes.drop_duplicates(inplace=True)
opiniones.drop_duplicates(inplace=True)

# Eliminar nulos críticos
opiniones.dropna(subset=["cliente_id", "producto_id", "fuente_id"], inplace=True)

# Normalización de formatos
opiniones["fecha"] = pd.to_datetime(opiniones["fecha"], errors="coerce")
productos["precio"] = productos["precio"].astype(float)

# Limpieza de textos
opiniones["comentario"] = opiniones["comentario"].astype(str).str.strip()

print("Datos transformados correctamente")

# =========================
# CARGA
# =========================
clientes.to_sql("Clientes", engine, if_exists="append", index=False)
productos.to_sql("Productos", engine, if_exists="append", index=False)
fuentes.to_sql("FuentesDatos", engine, if_exists="append", index=False)
opiniones.to_sql("Opiniones", engine, if_exists="append", index=False)

print("ETL ejecutado correctamente. Datos cargados en la base pruebin.")
