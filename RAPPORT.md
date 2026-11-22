# Rapport — Écosystème Big Data 2 (Jalon 1 : Parties 1 à 3)

Binôme : Aya Matou & ________ — EMSI 5e année — 2026–2027

> 10 pages maximum au total (6 parties). Les textes ci-dessous sont des bases à adapter
> avec VOS chiffres et VOS captures (marquées 📸). Parties 4–6 : jalon 2.

## Partie 1 — Cluster et positionnement (1 page max)

**Déploiement.** Docker Compose : Kafka (KRaft, topic `orders`, 3 partitions), Spark Master +
2 Workers (2 cœurs, 2 Go chacun), MinIO (bucket `lake`), Jupyter/PySpark.
📸 Spark Master UI : 2 Workers ALIVE · 📸 console MinIO : bucket `lake`
Sortie du script : version `3.5.3`, master `spark://spark-master:7077`, appName, `defaultParallelism = __`.

**Rôles.**
- *Driver* : processus qui exécute notre script PySpark (conteneur `jupyter`) ; il construit le DAG, planifie stages et tâches.
- *Cluster Manager* : le Master standalone (`spark-master:7077`) ; il alloue les ressources des Workers à l'application.
- *Executors* : processus lancés sur les Workers ; exécutent les tâches sur les partitions et gardent les données en cache.
- *SparkContext* : point d'entrée vers le cluster (connexion au Master, création de RDD, accumulateurs, broadcast).

**Deux limites de MapReduce corrigées par Spark.**
1. *Écritures disque entre chaque étape* : MapReduce persiste sur HDFS entre jobs enchaînés ; Spark garde les données en mémoire (`cache()` sur `delivered` en Partie 2, réutilisé pour CA, panier moyen et top 5).
2. *Modèle rigide map→reduce, un seul sens, pas d'interactif/streaming* : Spark offre un DAG d'opérations riches, SQL, et Structured Streaming — nécessaire pour traiter le flux Kafka `orders` en jalon 2.

## Partie 2 — SparkCore (RDD)

Chaîne : `textFile → map(json.loads) → filter(DELIVERED) → map + reduceByKey / aggregateByKey`.

📊 Tableau CA total et panier moyen par ville (voir `results/part2_resultats.json`) · Top 5 restaurants.

**Lineage** (`toDebugString`) : 📸 coller `results/part2_lineage.txt`.
Transformations (lazy) : `map`, `filter`, `reduceByKey`, `aggregateByKey`, `mapValues`.
Actions : `collect`, `takeOrdered`.

**DAG / stages.** 📸 DAG Spark UI — nombre de stages : __.
`reduceByKey` provoque un *shuffle* : les paires de même clé (ville) sont regroupées sur la même
partition, ce qui coupe le job en deux stages (map-side combine, écriture shuffle, lecture shuffle).
L'agrégation locale avant le shuffle réduit les données échangées.

**Tolérance aux pannes.** Le lineage mémorise la suite de transformations qui a produit chaque RDD ;
si une partition est perdue (Executor en panne), Spark la recalcule uniquement à partir des
partitions parentes, sans réplication des données.

## Partie 3 — Spark SQL et DataFrames

Schémas explicites `StructType` (sans inférence). CA par ville en DataFrame puis en SQL
(`createOrReplaceTempView`), jointure avec `restaurants.json`, CA par catégorie × ville.
Écriture Parquet partitionnée par `city` dans `lake/silver/orders_history/`. 📸 `mc ls --recursive`.

**UDF vs `when/otherwise`.** Temps mesurés : UDF __ s, natif __ s (`results/timings.json`).
La version native est plus rapide : Catalyst comprend l'expression et l'optimise (génération de
code Tungsten), alors que l'UDF Python est une boîte noire : sérialisation ligne par ligne
JVM ↔ Python et aucune optimisation possible.

**`explain(True)`** 📸 (`results/part3_explain.txt`) : identifier
- le *plan logique analysé* (Join, Filter, Relation),
- le *plan optimisé* par Catalyst (filtre poussé vers la source, projection des colonnes utiles),
- la *stratégie physique* : `BroadcastHashJoin` attendu (référentiel de 200 lignes < seuil de broadcast 10 Mo).

**RDD vs DataFrame vs Dataset.**
| | RDD | DataFrame | Dataset |
|---|---|---|---|
| Typage | objets Python/JVM, pas de schéma | colonnes, schéma vérifié à l'exécution | typé à la compilation (Scala/Java seulement) |
| Optimisation | aucune (code opaque) | Catalyst + Tungsten | Catalyst + Tungsten |
| Temps mesuré (ce projet) | __ s | __ s | non disponible en PySpark |

Conclusion : à volume égal, l'API structurée est plus concise et plus rapide ; le RDD reste utile
pour du traitement non structuré ou un contrôle fin du partitionnement.
