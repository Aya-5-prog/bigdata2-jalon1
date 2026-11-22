"""Partie 3 : Spark SQL et DataFrames."""
import json
import os
import time

from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

from common import (HISTORY_PATH, LAKE, RESTAURANTS_PATH, RESULTS_DIR, get_spark,
                    keep_ui_open, record_timing)

spark = get_spark("Projet-BigData2-Partie3-DataFrame")
os.makedirs(RESULTS_DIR, exist_ok=True)

# 1. Schemas explicites (pas d'inference)
orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("restaurant_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("status", StringType(), True),
    StructField("amount_mad", DoubleType(), True),
    StructField("items_count", IntegerType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("courier_id", StringType(), True),
    StructField("event_time", TimestampType(), True),
])
# ATTENTION : adapter ces noms de colonnes a votre restaurants.json si besoin
restaurants_schema = StructType([
    StructField("restaurant_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("category", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
])

orders = spark.read.schema(orders_schema).json(HISTORY_PATH)

# restaurants.json : tableau JSON ou JSON Lines, on detecte
first = spark.read.text(RESTAURANTS_PATH).first()[0].strip()
restaurants = (spark.read.schema(restaurants_schema)
               .option("multiLine", first.startswith("["))
               .json(RESTAURANTS_PATH)
               .withColumnRenamed("city", "restaurant_city"))

delivered = orders.filter(F.col("status") == "DELIVERED").cache()
delivered.count()  # materialise le cache pour des mesures comparables

# 2a. CA par ville : API DataFrame
t0 = time.time()
ca_df = (delivered.groupBy("city")
         .agg(F.round(F.sum("amount_mad"), 2).alias("ca_mad"),
              F.count("*").alias("nb_commandes"),
              F.round(F.avg("amount_mad"), 2).alias("panier_moyen"))
         .orderBy(F.desc("ca_mad")))
ca_df.show()
t_df = time.time() - t0
record_timing("part3_dataframe_ca_s", t_df)

# 2b. Meme calcul en SQL
t0 = time.time()
delivered.createOrReplaceTempView("orders_delivered")
spark.sql("""
    SELECT city, ROUND(SUM(amount_mad), 2) AS ca_mad
    FROM orders_delivered GROUP BY city ORDER BY ca_mad DESC
""").show()
record_timing("part3_sql_ca_s", time.time() - t0)

# 3. Jointure avec le referentiel : CA par categorie et par ville
joined = delivered.join(restaurants.select("restaurant_id", "name", "category"),
                        on="restaurant_id", how="left")
ca_cat_city = (joined.groupBy("city", "category")
               .agg(F.round(F.sum("amount_mad"), 2).alias("ca_mad"))
               .orderBy("city", F.desc("ca_mad")))
ca_cat_city.show(30, truncate=False)

# 4. UDF vs fonctions natives (when/otherwise)
@F.udf(StringType())
def distance_band(d):
    if d is None:
        return None
    return "court" if d < 2 else ("moyen" if d <= 5 else "long")

t0 = time.time()
with_udf = delivered.withColumn("band", distance_band("distance_km"))
with_udf.groupBy("band").count().show()
t_udf = time.time() - t0

t0 = time.time()
native = delivered.withColumn(
    "band",
    F.when(F.col("distance_km") < 2, "court")
     .when(F.col("distance_km") <= 5, "moyen")
     .otherwise("long"))
native.groupBy("band").count().show()
t_native = time.time() - t0
record_timing("part3_udf_s", t_udf)
record_timing("part3_when_otherwise_s", t_native)
print(f"UDF : {t_udf:.2f}s | when/otherwise : {t_native:.2f}s")

# 5. Ecriture Silver : nettoye, dedoublonne, partitionne par ville
clean = (orders
         .dropna(subset=["order_id", "status", "city", "event_time"])
         .dropDuplicates(["order_id", "status"])
         .withColumn("distance_band", F.when(F.col("distance_km") < 2, "court")
                     .when(F.col("distance_km") <= 5, "moyen").otherwise("long")))
(clean.write.mode("overwrite").partitionBy("city")
 .parquet(f"{LAKE}/silver/orders_history/"))
print("Parquet ecrit dans", f"{LAKE}/silver/orders_history/")

# 6. explain(True) sur la jointure + sauvegarde du plan
print("\n=== explain(True) de la jointure ===")
joined.explain(True)
with open(f"{RESULTS_DIR}/part3_explain.txt", "w") as f:
    f.write(joined._jdf.queryExecution().toString())

with open(f"{RESULTS_DIR}/part3_resultats.json", "w") as f:
    json.dump({"ca_par_ville": [r.asDict() for r in ca_df.collect()]},
              f, indent=2, ensure_ascii=False, default=str)

keep_ui_open()
spark.stop()
