#!/usr/bin/env python3
"""Generateur de commandes (equivalent du producer.py fourni).

    python3 producer.py --mode file  --n 50000          -> data/orders_history.jsonl + data/restaurants.json
    python3 producer.py --mode kafka --rate 20          -> topic Kafka "orders" (20 evt/s)

Un evenement JSON par changement d'etat : CREATED, PICKED_UP, DELIVERED, CANCELLED.
~5 % d'evenements en retard (jusqu'a 15 min) et ~2 % de doublons (meme order_id + meme status).
Le mode kafka necessite : pip install kafka-python
"""
import argparse
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone

CITIES = {"Casablanca": (33.57, -7.59), "Rabat": (34.02, -6.84), "Marrakech": (31.63, -8.00),
          "Tanger": (35.77, -5.80), "Fes": (34.03, -5.00), "Agadir": (30.42, -9.60)}
CATEGORIES = ["pizza", "tacos", "sushi", "marocain", "burger", "healthy"]
LATE_RATE, DUP_RATE = 0.05, 0.02


def make_restaurants(path):
    if os.path.exists(path):
        with open(path) as f:
            txt = f.read().strip()
        if txt.startswith("["):
            return json.loads(txt)
        return [json.loads(line) for line in txt.splitlines() if line.strip()]
    restaurants = []
    for i in range(200):
        city = random.choice(list(CITIES))
        lat, lon = CITIES[city]
        restaurants.append({"restaurant_id": f"R-{i:03d}", "name": f"Resto {i}", "city": city,
                            "category": random.choice(CATEGORIES),
                            "latitude": round(lat + random.uniform(-.05, .05), 5),
                            "longitude": round(lon + random.uniform(-.05, .05), 5)})
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        for r in restaurants:
            f.write(json.dumps(r) + "\n")
    return restaurants


class Generator:
    def __init__(self, restaurants):
        self.restaurants = restaurants
        self.active = {}      # order_id -> (order, status)
        self.counter = 0

    def next_event(self, now):
        """Cree une commande (CREATED) ou fait avancer une commande existante."""
        if not self.active or random.random() < 0.4:
            r = random.choice(self.restaurants)
            self.counter += 1
            order = {"order_id": f"ORD-{self.counter:07d}", "restaurant_id": r["restaurant_id"],
                     "customer_id": f"C-{random.randint(1, 20000):05d}", "city": r["city"],
                     "amount_mad": round(random.uniform(40, 350), 2),
                     "items_count": random.randint(1, 8),
                     "distance_km": round(random.uniform(0.3, 9), 1),
                     "courier_id": f"L-{r['city'][:3].upper()}-{random.randint(1, 15):02d}"}
            status = "CREATED"
            self.active[order["order_id"]] = (order, status)
        else:
            oid = random.choice(list(self.active))
            order, current = self.active[oid]
            if current == "CREATED":
                status = "CANCELLED" if random.random() < 0.08 else "PICKED_UP"
            else:
                status = "CANCELLED" if random.random() < 0.03 else "DELIVERED"
            if status in ("DELIVERED", "CANCELLED"):
                del self.active[oid]
            else:
                self.active[oid] = (order, status)
        t = now
        if random.random() < LATE_RATE:                       # evenement en retard
            t = now - timedelta(seconds=random.randint(60, 15 * 60))
        return dict(order, status=status, event_time=t.strftime("%Y-%m-%dT%H:%M:%SZ"))


def run_file(args):
    random.seed(42)
    restaurants = make_restaurants(os.path.join(args.out, "restaurants.json"))
    gen = Generator(restaurants)
    start = datetime.now(timezone.utc) - timedelta(days=3)
    step = timedelta(days=3) / args.n
    path = os.path.join(args.out, "orders_history.jsonl")
    written = 0
    with open(path, "w") as f:
        for i in range(args.n):
            ev = gen.next_event(start + step * i)
            f.write(json.dumps(ev) + "\n")
            written += 1
            if random.random() < DUP_RATE:                    # doublon
                f.write(json.dumps(ev) + "\n")
                written += 1
    print(f"{written} evenements ecrits dans {path}")


def run_kafka(args):
    from kafka import KafkaProducer
    random.seed()
    restaurants = make_restaurants(os.path.join(args.out, "restaurants.json"))
    gen = Generator(restaurants)
    producer = KafkaProducer(bootstrap_servers=args.bootstrap,
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                             key_serializer=lambda k: k.encode("utf-8"))
    print(f"Envoi de {args.rate} evenements/s vers '{args.topic}' (Ctrl+C pour arreter)")
    sent = 0
    try:
        while True:
            tick = time.time()
            for _ in range(args.rate):
                ev = gen.next_event(datetime.now(timezone.utc))
                producer.send(args.topic, key=ev["order_id"], value=ev)
                sent += 1
                if random.random() < DUP_RATE:
                    producer.send(args.topic, key=ev["order_id"], value=ev)
                    sent += 1
            producer.flush()
            if sent % (args.rate * 10) < args.rate:
                print(f"{sent} evenements envoyes")
            time.sleep(max(0, 1 - (time.time() - tick)))
    except KeyboardInterrupt:
        print(f"\nArret. Total : {sent} evenements")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["file", "kafka"], required=True)
    ap.add_argument("--n", type=int, default=50000, help="nombre d'evenements (mode file)")
    ap.add_argument("--rate", type=int, default=20, help="evenements par seconde (mode kafka)")
    ap.add_argument("--out", default="data", help="dossier de sortie")
    ap.add_argument("--bootstrap", default="localhost:29092")
    ap.add_argument("--topic", default="orders")
    a = ap.parse_args()
    run_file(a) if a.mode == "file" else run_kafka(a)