"""
gold/transform_gold.py — Agregaciones analíticas y enriquecimiento geográfico

Responsabilidad única:
  - Recibir el DataFrame Silver (viajes limpios)
  - Recibir el DataFrame de zonas (lookup desde Bronze)
  - Resolver el join geográfico una sola vez
  - Producir los cuatro modelos analíticos pre-agregados

NO sabe nada de MinIO.
NO sabe nada de Postgres.
NO lee ni escribe archivos.
El join geográfico ocurre aquí — no en Silver, no en el consumidor.
"""

import logging
import pandas as pd

logger = logging.getLogger("etl_pipeline.gold.transform")


# ─────────────────────────────────────────────
# JOIN GEOGRÁFICO
# ─────────────────────────────────────────────

def enrich_with_zones(df: pd.DataFrame, df_zones: pd.DataFrame) -> pd.DataFrame:
    """
    Resuelve el join geográfico entre viajes y zonas.

    Se hacen dos joins sobre el mismo lookup:
      - pulocationid → pickup_zone, pickup_borough
      - dolocationid → dropoff_zone, dropoff_borough

    Por qué se hace aquí y no en Silver:
      Silver no necesita geografía — es una capa de limpieza del viaje.
      Gold es quien responde preguntas de negocio que requieren
      contexto geográfico. El join se resuelve una sola vez acá
      y los consumidores nunca lo ven.

    Por qué LEFT JOIN y no INNER JOIN:
      Algunos viajes pueden tener location_id que no está en el lookup.
      Un INNER JOIN descartaría esos viajes silenciosamente.
      LEFT JOIN los preserva con zona = None — decisión explícita.
    """
    # Renombrar columnas del lookup para el join de pickup
    zones_pickup = df_zones.rename(columns={
        "LocationID": "pulocationid",
        "Zone": "pickup_zone",
        "Borough": "pickup_borough",
    })[["pulocationid", "pickup_zone", "pickup_borough"]]

    # Renombrar columnas del lookup para el join de dropoff
    zones_dropoff = df_zones.rename(columns={
        "LocationID": "dolocationid",
        "Zone": "dropoff_zone",
        "Borough": "dropoff_borough",
    })[["dolocationid", "dropoff_zone", "dropoff_borough"]]

    df = df.merge(zones_pickup, on="pulocationid", how="left")
    df = df.merge(zones_dropoff, on="dolocationid", how="left")

    logger.debug("Join geográfico aplicado: pickup_zone, pickup_borough, "
                 "dropoff_zone, dropoff_borough")
    return df


# ─────────────────────────────────────────────
# MODELOS ANALÍTICOS
# ─────────────────────────────────────────────

def build_hourly_demand(df: pd.DataFrame) -> pd.DataFrame:
    """
    Modelo 1 — Demanda por hora.

    Pregunta: ¿Cuántos viajes ocurren por hora del día
    y cómo varía entre semana y fin de semana?

    No requiere join geográfico.
    """
    df["pickup_date"] = df["tpep_pickup_datetime"].dt.date

    result = (
        df.groupby(["pickup_date", "pickup_hour", "is_weekend"])
        .agg(
            total_trips=("vendorid", "count"),
            total_passengers=("passenger_count", "sum"),
            avg_trip_distance=("trip_distance", "mean"),
            avg_fare=("fare_amount", "mean"),
            avg_duration_minutes=("trip_duration_minutes", "mean"),
        )
        .round(2)
        .reset_index()
    )

    logger.info(f"hourly_demand generado — {len(result):,} filas")
    return result


def build_zone_performance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Modelo 2 — Rendimiento por zona.

    Pregunta: ¿Qué zonas generan más ingresos y cuál es
    el perfil de viaje de cada una?

    Requiere join geográfico previo (pickup_zone, pickup_borough).
    """
    result = (
        df.groupby(["pickup_zone", "pickup_borough"])
        .agg(
            total_trips=("vendorid", "count"),
            total_revenue=("total_amount", "sum"),
            avg_fare=("fare_amount", "mean"),
            avg_tip_percentage=("tip_percentage", "mean"),
            avg_trip_distance=("trip_distance", "mean"),
        )
        .round(2)
        .reset_index()
    )

    logger.info(f"zone_performance generado — {len(result):,} filas")
    return result


def build_tip_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Modelo 3 — Análisis de propinas.

    Pregunta: ¿Qué factores están correlacionados
    con el monto de propina?

    No requiere join geográfico.
    """
    result = (
        df.groupby(["payment_type", "pickup_hour", "is_weekend"])
        .agg(
            total_trips=("vendorid", "count"),
            avg_tip_amount=("tip_amount", "mean"),
            avg_tip_percentage=("tip_percentage", "mean"),
        )
        .round(2)
        .reset_index()
    )

    logger.info(f"tip_analysis generado — {len(result):,} filas")
    return result


def build_daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Modelo 4 — Resumen diario.

    Pregunta: ¿Cómo fue el día operativamente?

    No requiere join geográfico.
    """
    df["pickup_date"] = df["tpep_pickup_datetime"].dt.date

    result = (
        df.groupby("pickup_date")
        .agg(
            total_trips=("vendorid", "count"),
            total_revenue=("total_amount", "sum"),
            total_passengers=("passenger_count", "sum"),
            avg_speed_mph=("speed_mph", "mean"),
            avg_trip_duration_minutes=("trip_duration_minutes", "mean"),
            avg_fare=("fare_amount", "mean"),
            avg_tip_percentage=("tip_percentage", "mean"),
        )
        .round(2)
        .reset_index()
    )

    logger.info(f"daily_summary generado — {len(result):,} filas")
    return result


# ─────────────────────────────────────────────
# PUNTO DE ENTRADA
# ─────────────────────────────────────────────

def run(df_silver: pd.DataFrame, df_zones: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Produce los cuatro modelos analíticos de Gold.

    Flujo:
      1. Enriquecer Silver con información geográfica (join)
      2. Construir cada modelo sobre el DataFrame enriquecido

    Args:
        df_silver: DataFrame limpio desde Silver
        df_zones:  DataFrame del lookup taxi_zone_lookup.csv

    Returns:
        dict con los cuatro modelos — claves coinciden con
        los nombres definidos en config.yml bajo gold.models
    """
    df_enriched = enrich_with_zones(df_silver, df_zones)

    models = {
        "hourly_demand":    build_hourly_demand(df_enriched),
        "zone_performance": build_zone_performance(df_enriched),
        "tip_analysis":     build_tip_analysis(df_enriched),
        "daily_summary":    build_daily_summary(df_enriched),
    }

    logger.info(f"Gold: {len(models)} modelos generados")
    return models