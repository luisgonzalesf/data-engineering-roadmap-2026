"""
main.py — Orquestador del pipeline ETL

── FASE-3: CAMBIOS ──────────────────────────────────────────
  Se extiende para orquestar las tres capas: Bronze, Silver y Gold.

  Cambios respecto a Fase-2:
    1. Se agrega etapa de extracción del lookup (taxi_zone_lookup.csv)
    2. Se agrega etapa SILVER: transform + load
    3. Se agrega etapa GOLD: transform + load
    4. El cliente S3 se construye una vez y se reutiliza en todas las etapas
    5. Bronze ya NO carga a PostgreSQL — solo persiste en MinIO
    6. PostgreSQL solo se escribe en Gold

  Lo que NO cambia:
    - Estructura de manejo de errores por etapa
    - Patrón sys.exit(1) en caso de fallo
    - Logger y configuración desde utils
    - Generador row_groups_pipeline para Bronze
─────────────────────────────────────────────────────────────
"""

import logging
import sys
from time import perf_counter

import pandas as pd

from utils import load_config, setup_logging
from bronze import extract

from silver import transform_silver, load_silver
from gold import transform_gold, load_gold

logger = setup_logging()

PANDAS_DTYPE_BACKEND = "numpy_nullable"


# ─────────────────────────────────────────────
# PIPELINE PRINCIPAL
# ─────────────────────────────────────────────

def run_pipeline(config_path: str = "config.yml") -> None:
    t_global = perf_counter()
    logger.info("=" * 60)
    logger.info("Pipeline ETL iniciado — Bronze → Silver → Gold")
    logger.info("=" * 60)

    # ── 1. Configuración ─────────────────────────────────────
    try:
        cfg = load_config(config_path)
        logger.info("Configuración cargada correctamente.")
    except (FileNotFoundError, EnvironmentError) as e:
        logger.error(f"Error de configuración: {e}")
        sys.exit(1)

    # ── FASE-3: Cliente S3 compartido ─────────────────────────
    # Se construye una vez y se pasa a todas las etapas.
    # Evita reconexiones innecesarias entre capas.
    s3 = extract.build_s3_client(cfg)

    # ── 2. Extract Bronze — Viajes ────────────────────────────
    logger.info("── ETAPA 1: EXTRACT BRONZE — Viajes ──")
    try:
        pqfile = extract.run(cfg)
    except Exception as e:
        logger.error(f"Fallo en EXTRACT (viajes): {e}", exc_info=True)
        sys.exit(1)

    # ── FASE-3: Extract Bronze — Lookup ───────────────────────
    logger.info("── ETAPA 2: EXTRACT BRONZE — Lookup ──")
    try:
        lookup_bytes = extract.run_lookup(cfg, s3=s3)
    except Exception as e:
        logger.error(f"Fallo en EXTRACT (lookup): {e}", exc_info=True)
        sys.exit(1)

    # ── 3. Bronze — solo MinIO ────────────────────────────────
    # Bronze persiste en MinIO durante extract.run().
    # No hay carga a PostgreSQL en esta capa — ocurre únicamente en Gold.

    # ── FASE-3: Transform + Load Silver ───────────────────────
    logger.info("── ETAPA 4: TRANSFORM + LOAD SILVER ──")
    try:
        df_bronze = load_silver.read_bronze(s3, cfg)
        df_silver = transform_silver.run(df_bronze)
        silver_metrics = load_silver.run(cfg, s3, df_silver)
    except Exception as e:
        logger.error(f"Fallo en SILVER: {e}", exc_info=True)
        sys.exit(1)

    # ── FASE-3: Transform + Load Gold ─────────────────────────
    logger.info("── ETAPA 5: TRANSFORM + LOAD GOLD ──")
    try:
        # Leer lookup desde bytes descargados en Extract
        import io
        df_zones = pd.read_csv(io.BytesIO(lookup_bytes))

        gold_models = transform_gold.run(df_silver, df_zones)
        gold_metrics = load_gold.run(cfg, s3, gold_models)
    except Exception as e:
        logger.error(f"Fallo en GOLD: {e}", exc_info=True)
        sys.exit(1)

    # ── 6. Reporte final ──────────────────────────────────────
    elapsed_total = perf_counter() - t_global
    logger.info("=" * 60)
    logger.info(
        f"Pipeline finalizado exitosamente | "
        f"silver_rows={silver_metrics['total_rows']:,} | "
        f"gold_models={len(gold_metrics)} | "
        f"tiempo_total={elapsed_total:.2f}s"
    )
    for model_name, m in gold_metrics.items():
        logger.info(f"  {model_name}: {m['rows']:,} filas → PostgreSQL")
    logger.info("=" * 60)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ETL Pipeline — NYC Taxi Lakehouse")
    parser.add_argument("--config", default="config.yml")
    args = parser.parse_args()
    run_pipeline(config_path=args.config)