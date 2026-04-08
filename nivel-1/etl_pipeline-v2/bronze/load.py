"""
load.py — Creación de esquema y carga masiva a PostgreSQL

Responsabilidad única:
  - Crear la tabla en Postgres (solo esquema, una vez)
  - Recibir DataFrames transformados y cargarlos via COPY
  - Gestionar la conexión y el cursor de forma segura
  - Registrar métricas por row group

NO transforma datos.
NO descarga ni lee desde MinIO.
"""

import io
import logging
from contextlib import contextmanager
from time import perf_counter

import psycopg2
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import pandas as pd

from utils import measure

logger = logging.getLogger("etl_pipeline.load")


# ─────────────────────────────────────────────
# CONEXIÓN
# ─────────────────────────────────────────────

def build_connection_string(cfg: dict) -> str:
    pg = cfg["postgres"]
    return (
        f"postgresql://{pg['user']}:{pg['password']}"
        f"@{pg['host']}:{pg['port']}/{pg['db']}"
    )


@contextmanager
def postgres_cursor(cfg: dict):
    """
    Context manager para conexión + cursor de psycopg2.
    Garantiza cierre seguro incluso si ocurre una excepción.
    """
    conn = psycopg2.connect(
        host=cfg["postgres"]["host"],
        port=cfg["postgres"]["port"],
        dbname=cfg["postgres"]["db"],
        user=cfg["postgres"]["user"],
        password=cfg["postgres"]["password"],
    )
    cursor = conn.cursor()
    try:
        yield conn, cursor
    finally:
        cursor.close()
        conn.close()
        logger.debug("Conexión a Postgres cerrada.")


# ─────────────────────────────────────────────
# ESQUEMA
# ─────────────────────────────────────────────

def create_table(cfg: dict, pqfile: pq.ParquetFile) -> list[str]:
    """
    Crea la tabla en Postgres usando slice(0,0) sobre PyArrow,
    idéntico al script original full-load.py.

    Por qué slice(0,0) y no read_row_group(0).to_pandas():
      Cuando pandas convierte un PyArrow int64 con nulls, lo degrada
      silenciosamente a float64 (numpy no puede representar NaN en int).
      Con datos reales, el row group 0 puede no tener nulls → pandas
      entrega int64 → SQLAlchemy crea BIGINT. Luego el row group 3
      tiene nulls → pandas entrega float64 → CSV produce "1.0" →
      Postgres rechaza "1.0" en BIGINT.

      slice(0,0) toma el schema de PyArrow con cero filas.
      Sin datos, no hay nulls que forzar a float.
      PyArrow declara los tipos originales del Parquet tal cual,
      y SQLAlchemy los mapea de forma estable y consistente.

      Este es exactamente el patrón que usa full-load.py y que
      funciona correctamente en producción.
    """
    table = cfg["postgres"]["table"]
    conn_str = build_connection_string(cfg)
    engine = create_engine(conn_str)

    # slice(0,0) → schema puro de PyArrow, sin datos, sin degradación de tipos
    df_schema = pqfile.read_row_group(0).slice(0, 0).to_pandas()
    df_schema.columns = [c.lower() for c in df_schema.columns]
    df_schema.to_sql(table, engine, if_exists="replace", index=False)
    engine.dispose()

    columns = list(df_schema.columns)
    logger.info(f"Tabla '{table}' creada con {len(columns)} columnas.")
    return columns


# ─────────────────────────────────────────────
# CARGA
# ─────────────────────────────────────────────

def copy_to_postgres(cursor, conn, table: str, df: pd.DataFrame) -> None:
    """
    Carga un DataFrame a Postgres usando COPY FROM STDIN.

    Por qué COPY y no INSERT:
      - COPY bypassea parsing fila a fila
      - Minimiza write-ahead logging
      - Velocidad: 10x-100x más rápido para volúmenes grandes
    """
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor.copy_expert(f"COPY {table} FROM STDIN WITH CSV", buffer)
    conn.commit()


def load_row_group(
    cursor,
    conn,
    cfg: dict,
    df: pd.DataFrame,
    group_index: int,
    total_groups: int,
) -> None:
    """
    Carga un row group transformado a Postgres.
    Registra tiempo y filas por grupo.
    """
    table = cfg["postgres"]["table"]
    t_start = perf_counter()

    copy_to_postgres(cursor, conn, table, df)

    elapsed = perf_counter() - t_start
    logger.info(
        f"Row group {group_index + 1}/{total_groups} | "
        f"{len(df):,} filas | {elapsed:.2f}s"
    )


# ─────────────────────────────────────────────
# PUNTO DE ENTRADA
# ─────────────────────────────────────────────

def run(cfg: dict, pqfile: pq.ParquetFile, row_groups_iter) -> dict:
    """
    Ejecuta la etapa de carga completa:
      1. Crea la tabla en Postgres desde el schema PyArrow (slice(0,0))
      2. Itera sobre row_groups_iter cargando cada uno
      3. Devuelve métricas del proceso
    """
    columns = create_table(cfg, pqfile)
    total_rows = 0
    total_groups = 0

    with measure("load") as m:
        with postgres_cursor(cfg) as (conn, cursor):
            for group_index, total_groups_count, df in row_groups_iter:
                load_row_group(cursor, conn, cfg, df, group_index, total_groups_count)
                total_rows += len(df)
                total_groups += 1

    m.rows_processed = total_rows
    m.extra = {"groups": total_groups, "columns": len(columns)}

    logger.info(f"Carga finalizada — {total_rows:,} filas en {total_groups} grupos.")
    return {"total_rows": total_rows, "total_groups": total_groups}