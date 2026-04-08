"""
silver/transform_silver.py — Limpieza, validación y enriquecimiento

Responsabilidad única:
  - Recibir el DataFrame completo leído desde Bronze
  - Aplicar filtros de calidad (eliminar filas inválidas)
  - Derivar columnas calculadas desde los datos existentes
  - Estandarizar valores categóricos (códigos → etiquetas)
  - Devolver un DataFrame limpio listo para ser persistido en Silver

NO sabe nada de MinIO.
NO sabe nada de Postgres.
NO lee ni escribe archivos.
NO recibe ni devuelve el lookup de zonas — eso es responsabilidad de Gold.
"""

import logging
import pandas as pd

logger = logging.getLogger("etl_pipeline.silver.transform")


# ─────────────────────────────────────────────
# FILTROS DE CALIDAD
# ─────────────────────────────────────────────

def filter_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina filas que no representan viajes válidos.

    Criterios de descarte:
      - passenger_count <= 0 o nulo     → viaje sin pasajeros
      - trip_distance <= 0 o nulo       → distancia inválida
      - fare_amount < 0                 → reversión contable
      - pickup o dropoff nulos          → sin fechas no hay viaje
      - dropoff anterior a pickup       → dato corrupto

    Por qué se filtran acá y no en Bronze:
      Bronze preserva el dato original sin decisiones de negocio.
      Decidir qué es un viaje válido es una regla de negocio —
      pertenece a Silver.
    """
    before = len(df)

    df = df[df["passenger_count"].notna() & (df["passenger_count"] > 0)]
    df = df[df["trip_distance"].notna() & (df["trip_distance"] > 0)]
    df = df[df["fare_amount"].notna() & (df["fare_amount"] >= 0)]
    df = df[df["tpep_pickup_datetime"].notna()]
    df = df[df["tpep_dropoff_datetime"].notna()]
    df = df[df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"]]

    after = len(df)
    discarded = before - after
    logger.info(f"Filtro de calidad: {before:,} → {after:,} filas ({discarded:,} descartadas)")

    return df.reset_index(drop=True)


# ─────────────────────────────────────────────
# COLUMNAS DERIVADAS
# ─────────────────────────────────────────────

def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula columnas nuevas a partir de los campos existentes.

    Columnas que se agregan:
      - trip_duration_minutes   → duración real del viaje
      - speed_mph               → velocidad promedio
      - pickup_hour             → hora del día (0–23)
      - pickup_day_of_week      → día de la semana (0=lunes, 6=domingo)
      - is_weekend              → True si sábado o domingo
      - tip_percentage          → propina como % de la tarifa base

    Por qué se calculan en Silver y no en Gold:
      Son atributos del viaje, no agregaciones de negocio.
      Gold los consume para agrupar y filtrar — no los recalcula.
    """
    duration = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60

    df["trip_duration_minutes"] = duration.round(2)

    # Evitar división por cero: viajes con duración 0 reciben speed NaN
    df["speed_mph"] = (
        df["trip_distance"] / (duration / 60)
    ).where(duration > 0)

    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
    df["pickup_day_of_week"] = df["tpep_pickup_datetime"].dt.dayofweek
    df["is_weekend"] = df["pickup_day_of_week"].isin([5, 6])

    # Evitar división por cero: fare_amount = 0 recibe tip_percentage NaN
    df["tip_percentage"] = (
        (df["tip_amount"] / df["fare_amount"]) * 100
    ).where(df["fare_amount"] > 0).round(2)

    logger.debug("Columnas derivadas agregadas: trip_duration_minutes, speed_mph, "
                 "pickup_hour, pickup_day_of_week, is_weekend, tip_percentage")
    return df


# ─────────────────────────────────────────────
# ESTANDARIZACIÓN DE VALORES
# ─────────────────────────────────────────────

# Mapeos de códigos numéricos a etiquetas legibles.
# Se definen como constantes del módulo para facilitar
# su mantenimiento sin tocar la lógica de las funciones.

PAYMENT_TYPE_MAP = {
    1: "credit_card",
    2: "cash",
    3: "no_charge",
    4: "dispute",
}

RATECODE_MAP = {
    1: "standard",
    2: "jfk",
    3: "newark",
    4: "nassau",
    5: "negotiated",
    6: "group_ride",
}


def standardize_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte códigos numéricos a etiquetas legibles
    y estandariza flags de texto a booleanos.

    Transformaciones:
      - payment_type     int → string  (1 → "credit_card")
      - ratecode_id      int → string  (1 → "standard")
      - store_and_fwd_flag "Y"/"N" → bool

    Los valores que no están en el mapeo se convierten a None
    mediante .map() — si el código no existe, no se inventa
    una etiqueta.
    """
    # Nombres en minúscula tal como los entrega normalize_column_names de Bronze:
    # RatecodeID → ratecodeid (sin guion bajo)
    # payment_type y store_and_fwd_flag no cambian porque ya vienen en minúscula
    df["payment_type"] = df["payment_type"].map(PAYMENT_TYPE_MAP)
    df["ratecodeid"] = df["ratecodeid"].map(RATECODE_MAP)
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].map({"Y": True, "N": False})

    logger.debug("Estandarización aplicada: payment_type, ratecodeid, store_and_fwd_flag")
    return df


# ─────────────────────────────────────────────
# PUNTO DE ENTRADA
# ─────────────────────────────────────────────

def run(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica todas las transformaciones Silver al DataFrame completo.

    A diferencia de Bronze (que procesa row group por row group),
    Silver recibe el DataFrame completo leído desde el Parquet de Bronze.

    Pipeline:
      0. Normalizar nombres de columnas a minúscula
         — Bronze persiste con nombres originales (RatecodeID, PULocationID...)
         — Silver necesita nombres consistentes antes de cualquier operación
      1. Filtrar filas inválidas
      2. Agregar columnas derivadas
      3. Estandarizar valores categóricos

    Args:
        df: DataFrame crudo leído desde Bronze

    Returns:
        DataFrame limpio y enriquecido, listo para persistir en Silver
    """
    df.columns = [c.lower() for c in df.columns]
    df = filter_invalid_rows(df)
    df = add_derived_columns(df)
    df = standardize_categoricals(df)
    return df