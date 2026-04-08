"""
extract.py — Extracción y persistencia en Bronze

Responsabilidad única:
  - Descargar el Parquet desde la fuente externa
  - Crear el bucket Bronze si no existe (idempotente)
  - Persistir el archivo crudo en MinIO
  - Devolver un ParquetFile listo para ser procesado

  ── FASE-3: CAMBIOS ──────────────────────────────────────────
  Se agrega run_lookup() — punto de entrada nuevo para descargar
  y persistir taxi_zone_lookup.csv en Bronze.
  El resto del módulo no cambia.
  ─────────────────────────────────────────────────────────────

NO transforma datos.
NO sabe nada de Postgres.
NO toma decisiones de negocio sobre el contenido.
"""

import io
import logging

import boto3
import requests
import pyarrow.parquet as pq

from utils import measure

logger = logging.getLogger("etl_pipeline.extract")


# ─────────────────────────────────────────────
# CLIENTE MINIO
# ─────────────────────────────────────────────

def build_s3_client(cfg: dict) -> boto3.client:
    """Construye y devuelve el cliente S3/MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=cfg["minio"]["endpoint"],
        aws_access_key_id=cfg["minio"]["access_key"],
        aws_secret_access_key=cfg["minio"]["secret_key"],
    )


# ─────────────────────────────────────────────
# BRONZE
# ─────────────────────────────────────────────

def ensure_bucket(s3, bucket: str) -> None:
    """Crea el bucket si no existe. Operación idempotente."""
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
        logger.info(f"Bucket '{bucket}' creado.")
    else:
        logger.info(f"Bucket '{bucket}' ya existe.")


def upload_to_bronze(s3, bucket: str, object_path: str, data: bytes) -> None:
    """Sube el archivo crudo a la capa Bronze."""
    s3.put_object(Bucket=bucket, Key=object_path, Body=data)
    logger.info(f"Archivo almacenado en Bronze → {bucket}/{object_path}")


def already_in_bronze(s3, bucket: str, object_path: str) -> bool:
    """Verifica si el objeto ya existe en Bronze (evita re-descargas)."""
    try:
        s3.head_object(Bucket=bucket, Key=object_path)
        return True
    except s3.exceptions.ClientError:
        return False


# ─────────────────────────────────────────────
# DESCARGA
# ─────────────────────────────────────────────

def download_file(url: str) -> bytes:
    """
    Descarga un archivo desde una URL.
    Lanza HTTPError si el servidor responde con error.

    ── FASE-3: CAMBIO ───────────────────────────────────────
    Se renombró download_parquet() → download_file() para que
    sea genérica y sirva tanto para el Parquet de viajes como
    para el CSV de zonas.
    ─────────────────────────────────────────────────────────
    """
    logger.info(f"Descargando desde: {url}")
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    logger.info(f"Descarga completa — {len(response.content) / (1024**2):.1f} MB")
    return response.content


# ─────────────────────────────────────────────
# PUNTO DE ENTRADA — VIAJES (sin cambios)
# ─────────────────────────────────────────────

def run(cfg: dict) -> pq.ParquetFile:
    """
    Ejecuta la etapa de extracción del dataset de viajes:
      1. Conecta a MinIO
      2. Garantiza que Bronze existe
      3. Descarga el Parquet (o reutiliza si ya está en Bronze)
      4. Devuelve un ParquetFile listo para transform/load

    Returns:
        pq.ParquetFile — objeto con acceso a row groups
    """
    s3 = build_s3_client(cfg)
    bucket = cfg["bronze"]["bucket"]
    obj_path = cfg["bronze"]["object_path"]

    ensure_bucket(s3, bucket)

    with measure("extract") as m:
        if already_in_bronze(s3, bucket, obj_path):
            logger.info("Parquet ya en Bronze. Saltando descarga.")
        else:
            raw_bytes = download_file(cfg["source"]["url"])
            upload_to_bronze(s3, bucket, obj_path, raw_bytes)

        obj = s3.get_object(Bucket=bucket, Key=obj_path)
        parquet_bytes = io.BytesIO(obj["Body"].read())
        pqfile = pq.ParquetFile(parquet_bytes)

    m.extra = {"num_row_groups": pqfile.num_row_groups}
    logger.info(f"ParquetFile listo — {pqfile.num_row_groups} row groups")

    return pqfile


# ─────────────────────────────────────────────
# FASE-3: PUNTO DE ENTRADA — LOOKUP (nuevo)
# ─────────────────────────────────────────────

def run_lookup(cfg: dict, s3=None) -> bytes:
    """
    ── FASE-3: FUNCIÓN NUEVA ────────────────────────────────
    Descarga y persiste taxi_zone_lookup.csv en Bronze.

    Reutiliza el mismo cliente S3 si ya fue construido por run(),
    evitando una segunda conexión innecesaria.

    Por qué devuelve bytes y no un DataFrame:
      - extract.py no transforma datos
      - Gold decide cómo leer el CSV (pd.read_csv desde BytesIO)
      - Mantiene la responsabilidad única del módulo

    Returns:
        bytes — contenido crudo del CSV
    ─────────────────────────────────────────────────────────
    """
    if s3 is None:
        s3 = build_s3_client(cfg)

    bucket = cfg["lookup"]["bucket"]
    obj_path = cfg["lookup"]["object_path"]

    ensure_bucket(s3, bucket)

    with measure("extract_lookup") as m:
        if already_in_bronze(s3, bucket, obj_path):
            logger.info("Lookup ya en Bronze. Saltando descarga.")
        else:
            raw_bytes = download_file(cfg["lookup"]["url"])
            upload_to_bronze(s3, bucket, obj_path, raw_bytes)

        obj = s3.get_object(Bucket=bucket, Key=obj_path)
        csv_bytes = obj["Body"].read()

    m.extra = {"size_kb": round(len(csv_bytes) / 1024, 1)}
    logger.info(f"Lookup listo — {len(csv_bytes) / 1024:.1f} KB")

    return csv_bytes