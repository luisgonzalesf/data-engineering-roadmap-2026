"""
extract.py — Extracción y persistencia en Bronze (MinIO)

Responsabilidad única:
  - Descargar el Parquet desde la fuente externa
  - Crear el bucket Bronze si no existe (idempotente)
  - Persistir el archivo crudo en MinIO
  - Devolver un ParquetFile listo para ser procesado

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
    """Sube el Parquet crudo a la capa Bronze."""
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

def download_parquet(url: str) -> bytes:
    """
    Descarga el Parquet desde la URL de origen.
    Lanza HTTPError si el servidor responde con error.
    """
    logger.info(f"Descargando desde: {url}")
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    logger.info(f"Descarga completa — {len(response.content) / (1024**2):.1f} MB")
    return response.content


# ─────────────────────────────────────────────
# PUNTO DE ENTRADA
# ─────────────────────────────────────────────

def run(cfg: dict) -> pq.ParquetFile:
    """
    Ejecuta la etapa de extracción completa:
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
            logger.info("Archivo ya en Bronze. Saltando descarga.")
        else:
            raw_bytes = download_parquet(cfg["source"]["url"])
            upload_to_bronze(s3, bucket, obj_path, raw_bytes)

        # Leer desde Bronze (siempre, independiente del origen)
        obj = s3.get_object(Bucket=bucket, Key=obj_path)
        parquet_bytes = io.BytesIO(obj["Body"].read())
        pqfile = pq.ParquetFile(parquet_bytes)

    m.extra = {"num_row_groups": pqfile.num_row_groups}
    logger.info(f"ParquetFile listo — {pqfile.num_row_groups} row groups")

    return pqfile