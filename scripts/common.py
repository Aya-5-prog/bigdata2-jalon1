"""Utilitaires communs : SparkSession connectee au cluster + acces MinIO (S3A)."""
import json
import os
import time

from pyspark.sql import SparkSession

LAKE = "s3a://lake"
HISTORY_PATH = f"{LAKE}/bronze/history/orders_history.jsonl"
RESTAURANTS_PATH = f"{LAKE}/reference/restaurants.json"
RESULTS_DIR = "/workspace/results"


def get_spark(app_name="BigData2"):
    spark = (
        SparkSession.builder.appName(app_name)
        .master("spark://spark-master:7077")
        # le driver tourne dans le conteneur jupyter : les workers doivent pouvoir le joindre
        .config("spark.driver.host", "jupyter")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def record_timing(name, seconds):
    """Garde les temps d'execution (Parties 2 et 3) pour la comparaison RDD vs DataFrame."""
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = f"{RESULTS_DIR}/timings.json"
    data = {}
    if os.path.exists(path):
        with open(path) as f:
            data = json.load(f)
    data[name] = round(seconds, 3)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def keep_ui_open():
    """KEEP_UI=300 -> garde l'application vivante 300 s pour capturer la Spark UI (port 4040)."""
    seconds = int(os.getenv("KEEP_UI", "0"))
    if seconds:
        print(f"\n>> Spark UI disponible sur http://localhost:4040 pendant {seconds}s")
        time.sleep(seconds)
