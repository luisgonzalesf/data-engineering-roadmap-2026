"""
utils.py — Logging, métricas y helpers compartidos

Responsabilidad única:
  - Configurar logging estructurado
  - Registrar métricas de tiempo y memoria por etapa
  - Proveer helpers reutilizables

NO contiene lógica de negocio.
NO sabe nada de MinIO, Postgres ni Parquet.
"""

import os
import time
import logging
import tracemalloc
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Optional

import yaml


# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configura logging estructurado con timestamp y nivel."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("etl_pipeline")


logger = setup_logging()


# ─────────────────────────────────────────────
# MÉTRICAS
# ─────────────────────────────────────────────

@dataclass
class StageMetrics:
    """Métricas de una etapa del pipeline."""
    stage: str
    elapsed_seconds: float = 0.0
    peak_memory_mb: float = 0.0
    rows_processed: int = 0
    extra: dict = field(default_factory=dict)

    def log(self):
        logger.info(
            f"[METRICS] stage={self.stage} | "
            f"time={self.elapsed_seconds:.2f}s | "
            f"peak_mem={self.peak_memory_mb:.1f}MB | "
            f"rows={self.rows_processed:,}"
            + (f" | {self.extra}" if self.extra else "")
        )


@contextmanager
def measure(stage: str, rows: int = 0, extra: Optional[dict] = None, track_memory: bool = False):
    """
    Context manager que mide tiempo (y opcionalmente memoria) de una etapa.

    track_memory=True solo para etapas livianas (extract, schema).
    NO usar en loops de carga masiva: tracemalloc instrumenta cada
    allocación Python, lo que penaliza severamente operaciones de I/O
    intensivo como COPY FROM STDIN sobre millones de filas.

    Uso:
        with measure("extract", track_memory=True) as m:
            ...
        with measure("load") as m:   # solo tiempo, sin overhead
            ...
    """
    if track_memory:
        tracemalloc.start()

    t_start = time.perf_counter()
    metrics = StageMetrics(stage=stage, rows_processed=rows, extra=extra or {})
    yield metrics

    elapsed = time.perf_counter() - t_start
    metrics.elapsed_seconds = elapsed

    if track_memory:
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        metrics.peak_memory_mb = peak / (1024 ** 2)

    metrics.rows_processed = rows
    metrics.log()


# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────

def load_config(path: str = "config.yml") -> dict:
    """Carga config.yml y resuelve variables de entorno para credenciales."""
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)

    cfg["minio"]["access_key"] = _require_env("MINIO_ROOT_USER")
    cfg["minio"]["secret_key"] = _require_env("MINIO_ROOT_PASSWORD")
    cfg["postgres"]["password"] = _require_env("POSTGRES_PASSWORD")

    return cfg


def _require_env(var: str) -> str:
    """Lee una variable de entorno obligatoria. Falla claro si falta."""
    value = os.getenv(var)
    if not value:
        raise EnvironmentError(
            f"Variable de entorno requerida no encontrada: '{var}'\n"
            f"Asegúrate de exportarla antes de correr el pipeline."
        )
    return value