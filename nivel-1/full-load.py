import pyarrow.parquet as pq
import pandas as pd
import psycopg2
import io
from time import time
from sqlalchemy import create_engine

# --------------------------------
# CONFIG
# --------------------------------
PARQUET_FILE = "../yellow_tripdata_2024-01.parquet"
TABLE_NAME = "yellow_taxi_data"

# --------------------------------
# 1) Crear tabla automáticamente (solo esquema)
# --------------------------------
engine = create_engine(
    "postgresql://root:root@localhost:5432/ny_taxi"
)

# Leer solo el esquema desde Parquet
df_schema = (
    pq.read_table(PARQUET_FILE)
      .slice(0, 0)
      .to_pandas()
)

df_schema.to_sql(
    name=TABLE_NAME,
    con=engine,
    if_exists="replace",
    index=False
)

print("✔ Tabla creada automáticamente (solo esquema)\n")

# --------------------------------
# 2) Preparar lectura del Parquet
# --------------------------------
pqfile = pq.ParquetFile(PARQUET_FILE)
num_groups = pqfile.num_row_groups

print(f"Parquet detectado con {num_groups} row groups\n")

# --------------------------------
# 3) Conexión nativa a Postgres
# --------------------------------
conn = psycopg2.connect(
    host="localhost",
    dbname="ny_taxi",
    user="root",
    password="root"
)
cursor = conn.cursor()

# --------------------------------
# 4) Procesamiento por row groups
# --------------------------------
for i in range(num_groups):
    t_start = time()
    print(f"→ Procesando row group {i + 1}/{num_groups}")

    # Leer row group
    table = pqfile.read_row_group(i)
    df = table.to_pandas()

    # --------------------------------
    # Normalización mínima de tipos
    # --------------------------------
    for col in df.columns:
        if df[col].dtype == "float64":
            if ((df[col] % 1 == 0) | df[col].isnull()).all():
                df[col] = df[col].astype("Int64")

    for col in df.columns:
        if "datetime" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # --------------------------------
    # COPY FROM STDIN
    # --------------------------------
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor.copy_expert(
        f"COPY {TABLE_NAME} FROM STDIN WITH CSV",
        buffer
    )
    conn.commit()

    t_end = time()
    print(
        f"✔ Row group {i + 1} cargado en "
        f"{t_end - t_start:.2f} segundos\n"
    )

# --------------------------------
# 5) Cierre de recursos
# --------------------------------
cursor.close()
conn.close()

print("✔ Carga completa finalizada")