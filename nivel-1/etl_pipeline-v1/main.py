"""
main.py — Orquestador del pipeline ETL
"""

import logging
import sys
from time import perf_counter

import pyarrow.parquet as pq

from utils import load_config, setup_logging
import extract
import transform
import load

logger = setup_logging()

# dtype_backend consistente en todo el pipeline.
# "numpy_nullable" preserva int64 como Int64 (nullable) en lugar de
# degradarlo a float64 cuando hay nulls — evita que "1.0" llegue a
# una columna BIGINT y sea rechazado por Postgres.
PANDAS_DTYPE_BACKEND = "numpy_nullable"


def row_groups_pipeline(pqfile: pq.ParquetFile):
    """
    Generador que itera sobre los row groups del ParquetFile,
    aplica transform.run() a cada uno y los yield para load.
    """
    total = pqfile.num_row_groups
    for i in range(total):
        df_raw = pqfile.read_row_group(i).to_pandas()
        df_clean = transform.run(df_raw)
        yield i, total, df_clean


def run_pipeline(config_path: str = "config.yml") -> None:
    t_global = perf_counter()
    logger.info("=" * 60)
    logger.info("Pipeline ETL iniciado")
    logger.info("=" * 60)

    # ── 1. Configuración ─────────────────────────────────────
    try:
        cfg = load_config(config_path)
        logger.info("Configuración cargada correctamente.")
    except (FileNotFoundError, EnvironmentError) as e:
        logger.error(f"Error de configuración: {e}")
        sys.exit(1)

    # ── 2. Extract ────────────────────────────────────────────
    logger.info("── ETAPA 1: EXTRACT ──")
    try:
        pqfile = extract.run(cfg)
    except Exception as e:
        logger.error(f"Fallo en EXTRACT: {e}", exc_info=True)
        sys.exit(1)

    # ── 3. Load ───────────────────────────────────────────────
    logger.info("── ETAPA 2: TRANSFORM + LOAD ──")
    try:
        metrics = load.run(
            cfg=cfg,
            pqfile=pqfile,
            row_groups_iter=row_groups_pipeline(pqfile),
        )
    except Exception as e:
        logger.error(f"Fallo en LOAD: {e}", exc_info=True)
        sys.exit(1)

    # ── 4. Reporte final ──────────────────────────────────────
    elapsed_total = perf_counter() - t_global
    logger.info("=" * 60)
    logger.info(
        f"Pipeline finalizado exitosamente | "
        f"filas={metrics['total_rows']:,} | "
        f"grupos={metrics['total_groups']} | "
        f"tiempo_total={elapsed_total:.2f}s"
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ETL Pipeline — NYC Taxi Lakehouse")
    parser.add_argument("--config", default="config.yml")
    args = parser.parse_args()
    run_pipeline(config_path=args.config)