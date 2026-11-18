import argparse
import hashlib
import os
import re
from pathlib import Path

ROOT = Path("/pipeline")
BUCKET = "steam-reviews"


def object_store():
    from minio import Minio

    return Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=False,
    )


def upload(source):
    from minio.error import S3Error

    directory = ROOT / "data" / source
    paths = sorted(directory.glob("*.csv"))
    if not paths:
        raise ValueError(f"No CSV files in {directory}. Extract the Kaggle dataset there.")
    client = object_store()
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
    for path in paths:
        with path.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        object_name = f"raw/{source}/{digest}.csv"
        try:
            client.stat_object(BUCKET, object_name)
        except S3Error as error:
            if error.code != "NoSuchKey":
                raise
            client.fput_object(BUCKET, object_name, str(path), content_type="text/csv")
        print(f"{path.name} -> {object_name}", flush=True)
        yield f"s3a://{BUCKET}/{object_name}"


def spark_session(app_name):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .master(f"local[{os.environ['SPARK_THREADS']}]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.environ["SPARK_SHUFFLE_PARTITIONS"])
        .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
        .getOrCreate()
    )


def processed_path(source, snapshot, table):
    if source not in {"demo", "raw"} or not re.fullmatch(r"[a-f0-9]{20}", snapshot):
        raise ValueError("Invalid source or snapshot")
    return f"s3a://{BUCKET}/processed/{source}/{snapshot}/{table}"


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, choices=["demo", "raw"])
    parser.add_argument("--snapshot", required=True)
    return parser.parse_args()
