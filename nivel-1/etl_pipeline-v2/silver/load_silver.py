"""
silver/load_silver.py — Lectura desde Bronze y escritura en Silver

Responsabilidad única:
  - Leer el Parquet de viajes desde Bronze (MinIO)
  - Garantizar que el bucket Silver existe
  - Persistir el DataFrame transformado como Parquet en Silver

NO transforma datos — eso es responsabilidad de transform_silver.py.
NO sabe nada de Postgres — Silver no es una capa de consumo.
"""

import io
import logging

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from utils import measure

logger = logging.getLogger("etl_pipeline.silver.load")


# ─────────────────────────────────────────────
# LECTURA DESDE BRONZE
# ─────────────────────────────────────────────

def read_bronze(s3, cfg: dict) -> pd.DataFrame:
    """
    Lee el Parquet de viajes completo desde Bronze y lo devuelve
    como DataFrame.

    Por qué se lee completo en Silver (no por row groups):
      Bronze procesa por row groups para controlar memoria durante
      la ingesta inicial de un archivo grande desde HTTP.
      Silver lee desde MinIO — la red es local y el archivo ya está
      en el lago. Leerlo completo simplifica las transformaciones
      que necesitan visibilidad de todo el dataset (como los filtros
      que usan .reset_index()).
    """
    bucket = cfg["bronze"]["bucket"]
    obj_path = cfg["bronze"]["object_path"]

    logger.info(f"Leyendo Bronze → {bucket}/{obj_path}")
    obj = s3.get_object(Bucket=bucket, Key=obj_path)
    parquet_bytes = io.BytesIO(obj["Body"].read())
    df = pq.read_table(parquet_bytes).to_pandas()

    logger.info(f"Bronze leído — {len(df):,} filas")
    return df


# ─────────────────────────────────────────────
# ESCRITURA EN SILVER
# ─────────────────────────────────────────────

def ensure_bucket(s3, bucket: str) -> None:
    """Crea el bucket Silver si no existe. Operación idempotente."""
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
        logger.info(f"Bucket '{bucket}' creado.")
    else:
        logger.info(f"Bucket '{bucket}' ya existe.")


def write_silver(s3, cfg: dict, df: pd.DataFrame) -> None:
    """
    Persiste el DataFrame limpio en Silver como Parquet.

    Por qué Parquet y no CSV:
      - Tipado preservado — los tipos calculados en Silver
        (bool, int, float) se mantienen al releer
      - Compresión — Silver es más pequeño que Bronze porque
        las filas inválidas ya fueron eliminadas
      - Gold puede leer Silver con pq.read_table() de forma eficiente
    """
    bucket = cfg["silver"]["bucket"]
    obj_path = cfg["silver"]["object_path"]

    # Convertir DataFrame a Parquet en memoria
    table = pa.Table.from_pandas(df, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    s3.put_object(Bucket=bucket, Key=obj_path, Body=buffer.read())
    logger.info(f"Silver escrito → {bucket}/{obj_path} ({len(df):,} filas)")


# ─────────────────────────────────────────────
# PUNTO DE ENTRADA
# ─────────────────────────────────────────────

def run(cfg: dict, s3, df_silver: pd.DataFrame) -> dict:
    """
    Ejecuta la etapa de carga Silver completa:
      1. Garantiza que el bucket Silver existe
      2. Persiste el DataFrame transformado como Parquet

    Recibe df_silver ya transformado desde main.py —
    load_silver no llama a transform_silver directamente.

    Args:
        cfg:       configuración del pipeline
        s3:        cliente MinIO ya construido (reutilizado desde extract)
        df_silver: DataFrame limpio desde transform_silver.run()

    Returns:
        dict con métricas del proceso
    """
    ensure_bucket(s3, cfg["silver"]["bucket"])

    with measure("load_silver") as m:
        write_silver(s3, cfg, df_silver)

    m.rows_processed = len(df_silver)
    m.extra = {"columns": len(df_silver.columns)}

    logger.info(f"Silver finalizado — {len(df_silver):,} filas, "
                f"{len(df_silver.columns)} columnas")

    return {"total_rows": len(df_silver), "columns": len(df_silver.columns)}