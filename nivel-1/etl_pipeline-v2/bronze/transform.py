"""
transform.py — Normalización de tipos y reglas de negocio

Responsabilidad única:
  - Recibir un DataFrame crudo (desde un row group)
  - Normalizar nombres de columnas (lowercase)
  - Convertir float64 → Int64 cuando corresponde (enteros con nulls)
  - Parsear columnas datetime
  - Devolver un DataFrame listo para COPY

NO sabe nada de MinIO.
NO sabe nada de Postgres.
NO descarga ni persiste datos.
"""

import logging
import pandas as pd

logger = logging.getLogger("etl_pipeline.transform")


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte todos los nombres de columna a minúscula.

    Por qué:
      - Postgres es case-sensitive con comillas
      - Evita inconsistencias entre motores
      - Establece un contrato interno del pipeline
    """
    df.columns = [c.lower() for c in df.columns]
    return df


def cast_integer_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte float64 → Int64 cuando todos los valores son enteros o nulos.

    Por qué:
      PyArrow no lee tipos consistentemente entre row groups cuando hay nulls.
      Un row group sin nulls entrega passenger_count como int64 → Postgres
      crea la columna como BIGINT. Un row group posterior con nulls la entrega
      como float64 → COPY escribe "1.0" → Postgres rechaza con
      'invalid input syntax for type bigint'.

      Esta conversión replica exactamente el patrón del full-load.py original
      y garantiza consistencia en todos los row groups.
    """
    for col in df.columns:
        if df[col].dtype == "float64":
            if ((df[col] % 1 == 0) | df[col].isnull()).all():
                df[col] = df[col].astype("Int64")
                logger.debug(f"Columna '{col}' convertida float64 → Int64")
    return df


def cast_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parsea columnas con 'datetime' en el nombre como timestamps.

    Usa errors='coerce' para que valores inválidos se conviertan
    en NaT en lugar de lanzar excepciones.
    """
    datetime_cols = [c for c in df.columns if "datetime" in c.lower()]
    for col in datetime_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")
        logger.debug(f"Columna '{col}' parseada como datetime")
    return df


def run(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica todas las transformaciones a un DataFrame (un row group).

    Pipeline:
      1. Normalizar nombres de columnas
      2. Convertir float64 → Int64 (enteros con nulls)
      3. Parsear columnas datetime

    Args:
        df: DataFrame crudo desde pyarrow

    Returns:
        DataFrame transformado y listo para carga
    """
    df = normalize_column_names(df)
    df = cast_integer_columns(df)
    df = cast_datetime_columns(df)
    return df