"""
gold/load_gold.py — Escritura en MinIO y carga a PostgreSQL

Responsabilidad única:
  - Garantizar que el bucket Gold existe
  - Persistir cada modelo como Parquet en MinIO
  - Cargar cada modelo a PostgreSQL vía COPY FROM STDIN
  - Gold es el único punto del pipeline que escribe en PostgreSQL

NO transforma datos — eso es responsabilidad de transform_gold.py.
"""

import io
import logging
from time import perf_counter

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from sqlalchemy import create_engine

from utils import measure
from bronze.load import build_connection_string, postgres_cursor

logger = logging.getLogger("etl_pipeline.gold.load")


# ─────────────────────────────────────────────
# BUCKET GOLD
# ─────────────────────────────────────────────

def ensure_bucket(s3, bucket: str) -> None:
    """Crea el bucket Gold si no existe. Operación idempotente."""
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
        logger.info(f"Bucket '{bucket}' creado.")
    else:
        logger.info(f"Bucket '{bucket}' ya existe.")


# ─────────────────────────────────────────────
# ESCRITURA EN MINIO
# ─────────────────────────────────────────────

def write_parquet(s3, bucket: str, object_path: str, df: pd.DataFrame) -> None:
    """
    Persiste un modelo Gold como Parquet en MinIO.

    Los modelos Gold son DataFrames pequeños (agregaciones),
    por lo que la escritura completa en memoria es apropiada.
    """
    table = pa.Table.from_pandas(df, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    s3.put_object(Bucket=bucket, Key=object_path, Body=buffer.read())
    logger.info(f"Parquet escrito → {bucket}/{object_path}")


# ─────────────────────────────────────────────
# CARGA A POSTGRESQL
# ─────────────────────────────────────────────

def create_table(cfg: dict, table_name: str, df: pd.DataFrame) -> None:
    """
    Crea la tabla en PostgreSQL desde el schema del DataFrame.

    Reutiliza el patrón de load.py (Fase-2):
      - slice(0, 0) para obtener schema puro sin datos
      - SQLAlchemy para inferir y mapear tipos correctamente
      - if_exists="replace" para idempotencia

    Los modelos Gold son pequeños y con tipos simples
    (int, float, string, bool, date) — el mapeo de SQLAlchemy
    es estable y no requiere los trucos de tipos de Bronze.
    """
    conn_str = build_connection_string(cfg)
    engine = create_engine(conn_str)

    df_schema = df.iloc[0:0]  # schema puro sin filas
    df_schema.to_sql(table_name, engine, if_exists="replace", index=False)
    engine.dispose()

    logger.info(f"Tabla '{table_name}' creada en PostgreSQL")


def copy_to_postgres(cursor, conn, table_name: str, df: pd.DataFrame) -> None:
    """
    Carga el modelo a PostgreSQL usando COPY FROM STDIN.

    Reutiliza el mismo mecanismo de load.py (Fase-2).
    Los modelos Gold son pequeños — el buffer completo
    en memoria es apropiado y eficiente.
    """
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", buffer)
    conn.commit()


def load_model(cfg: dict, table_name: str, df: pd.DataFrame) -> None:
    """
    Crea la tabla y carga un modelo Gold a PostgreSQL.
    Gestiona la conexión de forma segura con context manager.
    """
    t_start = perf_counter()

    create_table(cfg, table_name, df)

    with postgres_cursor(cfg) as (conn, cursor):
        copy_to_postgres(cursor, conn, table_name, df)

    elapsed = perf_counter() - t_start
    logger.info(f"'{table_name}' cargado — {len(df):,} filas | {elapsed:.2f}s")


# ─────────────────────────────────────────────
# PUNTO DE ENTRADA
# ─────────────────────────────────────────────

def run(cfg: dict, s3, models: dict[str, pd.DataFrame]) -> dict:
    """
    Ejecuta la etapa de carga Gold completa:
      1. Garantiza que el bucket Gold existe
      2. Por cada modelo:
         a. Persiste como Parquet en MinIO
         b. Crea la tabla y carga a PostgreSQL vía COPY

    Gold es el único punto del pipeline que escribe en PostgreSQL.

    Args:
        cfg:    configuración del pipeline
        s3:     cliente MinIO ya construido
        models: dict de DataFrames desde transform_gold.run()
                — claves coinciden con gold.models en config.yml

    Returns:
        dict con métricas por modelo
    """
    ensure_bucket(s3, cfg["gold"]["bucket"])

    metrics = {}

    with measure("load_gold") as m:
        for model_name, df in models.items():
            model_cfg = cfg["gold"]["models"][model_name]
            bucket = cfg["gold"]["bucket"]

            # 1. Persistir en MinIO
            write_parquet(s3, bucket, model_cfg["object_path"], df)

            # 2. Cargar a PostgreSQL
            load_model(cfg, model_cfg["table"], df)

            metrics[model_name] = {"rows": len(df)}

    m.extra = {"models": list(models.keys())}
    logger.info(f"Gold finalizado — {len(models)} modelos cargados a PostgreSQL")

    return metrics