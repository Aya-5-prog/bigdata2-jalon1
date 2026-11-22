"""Partie 1 : SparkSession connectee au Master + infos du cluster."""
from common import get_spark, keep_ui_open

spark = get_spark("Projet-BigData2-Partie1")
sc = spark.sparkContext
sc.parallelize(range(100)).count()   # force le demarrage des Executors

print("=" * 60)
print("Version de Spark        :", spark.version)
print("Master                  :", sc.master)
print("appName                 :", sc.appName)
print("defaultParallelism      :", sc.defaultParallelism, "(coeurs disponibles)")
print("=" * 60)

keep_ui_open()
spark.stop()