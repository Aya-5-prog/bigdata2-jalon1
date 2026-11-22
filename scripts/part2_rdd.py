"""Partie 2 : traitement batch avec SparkCore (RDD) sur orders_history.jsonl."""
import json
import time

from common import HISTORY_PATH, RESULTS_DIR, get_spark, keep_ui_open, record_timing

import os

spark = get_spark("Projet-BigData2-Partie2-RDD")
sc = spark.sparkContext
os.makedirs(RESULTS_DIR, exist_ok=True)

t0 = time.time()

# 1. Chargement + parsing (transformations lazy)
raw = sc.textFile(HISTORY_PATH)
orders = raw.map(json.loads)

# 2. Uniquement les commandes livrees
delivered = orders.filter(lambda o: o["status"] == "DELIVERED").cache()

# 3. Chiffre d'affaires total par ville (map + reduceByKey)
ca_by_city = (
    delivered.map(lambda o: (o["city"], o["amount_mad"]))
    .reduceByKey(lambda a, b: a + b)
)

# 4. Panier moyen par ville : (somme, nombre) en un seul passage
sum_count = delivered.map(lambda o: (o["city"], o["amount_mad"])).aggregateByKey(
    (0.0, 0),
    lambda acc, v: (acc[0] + v, acc[1] + 1),
    lambda a, b: (a[0] + b[0], a[1] + b[1]),
)
avg_basket = sum_count.mapValues(lambda sc_: sc_[0] / sc_[1])

# 5. Top 5 restaurants par nombre de commandes livrees
top5 = (
    delivered.map(lambda o: (o["restaurant_id"], 1))
    .reduceByKey(lambda a, b: a + b)
    .takeOrdered(5, key=lambda kv: -kv[1])
)

# --- Actions : c'est ici que le calcul se declenche ---
ca_rows = sorted(ca_by_city.collect(), key=lambda kv: -kv[1])
avg_rows = dict(avg_basket.collect())
count_rows = {c: n for c, (s, n) in sum_count.collect()}

elapsed = time.time() - t0
record_timing("part2_rdd_total_s", elapsed)

print("\n=== CA total et panier moyen par ville (commandes DELIVERED) ===")
print(f"{'Ville':<15}{'CA (MAD)':>15}{'Nb cmd':>10}{'Panier moyen':>16}")
lines = []
for city, ca in ca_rows:
    print(f"{city:<15}{ca:>15,.2f}{count_rows[city]:>10}{avg_rows[city]:>16,.2f}")
    lines.append({"city": city, "ca_mad": round(ca, 2), "nb": count_rows[city],
                  "panier_moyen": round(avg_rows[city], 2)})

print("\n=== Top 5 restaurants (commandes livrees) ===")
for rid, n in top5:
    print(f"{rid:<10}{n:>8}")

print("\n=== Lineage (toDebugString) du CA par ville ===")
lineage = ca_by_city.toDebugString().decode("utf-8")
print(lineage)

print("Transformations (lazy) : textFile, map, filter, cache, reduceByKey, aggregateByKey, mapValues")
print("Actions (declenchent le job) : collect, takeOrdered")
print(f"\nTemps total RDD : {elapsed:.2f}s")

with open(f"{RESULTS_DIR}/part2_resultats.json", "w") as f:
    json.dump({"ca_par_ville": lines, "top5": top5, "temps_s": round(elapsed, 3)},
              f, indent=2, ensure_ascii=False)
with open(f"{RESULTS_DIR}/part2_lineage.txt", "w") as f:
    f.write(lineage)

keep_ui_open()
spark.stop()
