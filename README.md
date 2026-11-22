# Projet Écosystème Big Data 2 — Jalon 1 (Parties 1 à 3)

Plateforme Big Data pour un SaaS multi-tenant : Kafka, Spark 3.5.3, MinIO (S3), Jupyter.
Ce dépôt couvre le **jalon 1** : cluster, traitement RDD (SparkCore), Spark SQL / DataFrames.

## Versions
| Composant | Version |
|---|---|
| Spark | 3.5.3 (image `apache/spark:3.5.3-python3`) |
| Kafka | 3.7.0 (mode KRaft) |
| MinIO / mc | `latest` |
| hadoop-aws / aws-java-sdk-bundle | 3.3.4 / 1.12.262 (téléchargés automatiquement) |

## Arborescence
```
docker-compose.yml     Kafka, Spark Master + 2 Workers, MinIO, Jupyter
Dockerfile.jupyter     Jupyter + PySpark (même Python que les workers)
data/                  producer.py (prof), restaurants.json, orders_history.jsonl
scripts/               common.py, part1_session.py, part2_rdd.py, part3_dataframe.py
results/               sorties JSON/TXT générées (timings, explain, lineage)
RAPPORT.md             squelette du rapport
```

## Lancement

### 1. Démarrer le cluster
```bash
docker compose up -d --build
docker compose logs kafka-init minio-init     # topic "orders" (3 partitions) + bucket "lake"
```
Vérifications : Spark Master http://localhost:8080 (2 Workers `ALIVE`), console MinIO
http://localhost:9001 (`minioadmin` / `minioadmin`, bucket `lake`), Jupyter http://localhost:8888.

### 2. Générer l'historique (50 000 commandes)
```bash
python3 data/producer.py --mode file --n 50000     # générateur fourni -> orders_history.jsonl
# (secours si producer.py indisponible)  python3 scripts/gen_fallback.py --n 50000
```
Placer `orders_history.jsonl` et `restaurants.json` dans `./data/`, puis les charger dans MinIO :
```bash
docker compose run --rm --entrypoint sh minio-init -c "\
  mc alias set local http://minio:9000 minioadmin minioadmin && \
  mc cp /data/orders_history.jsonl local/lake/bronze/history/orders_history.jsonl && \
  mc cp /data/restaurants.json local/lake/reference/restaurants.json && \
  mc ls --recursive local/lake/"
```

### 3. Exécuter les parties (depuis le conteneur Jupyter)
```bash
docker compose exec jupyter python /workspace/scripts/part1_session.py
KEEP_UI=300 docker compose exec -e KEEP_UI=300 jupyter python /workspace/scripts/part2_rdd.py
docker compose exec -e KEEP_UI=300 jupyter python /workspace/scripts/part3_dataframe.py
```
`KEEP_UI=300` garde la Spark UI du driver ouverte 5 min sur http://localhost:4040 :
c'est le moment de capturer le **DAG** et les **stages** (onglets Jobs / Stages).

### 4. Preuves à capturer
- Spark Master UI avec 2 Workers actifs (port 8080)
- DAG + stages Spark UI (Parties 2 et 3, port 4040)
- Sortie `explain(True)` (aussi dans `results/part3_explain.txt`)
- `mc ls --recursive local/lake/` : fichiers Parquet partitionnés par ville dans `silver/orders_history/`

## Dépannage
- *Python in worker has different version* : reconstruire avec `docker compose build --no-cache jupyter`.
- Les noms de colonnes de `restaurants.json` sont supposés : `restaurant_id, name, city, category,
  latitude, longitude` — à adapter dans `scripts/part3_dataframe.py` si différents.
- Premier lancement lent : Spark télécharge hadoop-aws (mis en cache ensuite).
