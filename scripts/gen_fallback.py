"""Generateur de SECOURS (a utiliser seulement si producer.py du prof n'est pas dispo).

Produit data/restaurants.json (200 restaurants, JSON Lines) et data/orders_history.jsonl
avec ~5 % d'evenements en retard et ~2 % de doublons, comme le generateur officiel.

    python3 scripts/gen_fallback.py --n 50000
"""
import argparse
import json
import os
import random
from datetime import datetime, timedelta, timezone

CITIES = {"Casablanca": (33.57, -7.59), "Rabat": (34.02, -6.84), "Marrakech": (31.63, -8.0),
          "Tanger": (35.77, -5.80), "Fes": (34.03, -5.0), "Agadir": (30.42, -9.60)}
CATEGORIES = ["pizza", "tacos", "sushi", "marocain", "burger", "healthy"]
STATUSES = ["CREATED", "PICKED_UP", "DELIVERED", "CANCELLED"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=50000)
    ap.add_argument("--out", default="data")
    args = ap.parse_args()
    random.seed(42)
    os.makedirs(args.out, exist_ok=True)

    restaurants = []
    for i in range(200):
        city = random.choice(list(CITIES))
        lat, lon = CITIES[city]
        restaurants.append({"restaurant_id": f"R-{i:03d}", "name": f"Resto {i}", "city": city,
                            "category": random.choice(CATEGORIES),
                            "latitude": round(lat + random.uniform(-.05, .05), 5),
                            "longitude": round(lon + random.uniform(-.05, .05), 5)})
    with open(f"{args.out}/restaurants.json", "w") as f:
        for r in restaurants:
            f.write(json.dumps(r) + "\n")

    now = datetime.now(timezone.utc)
    events = []
    for i in range(args.n):
        r = random.choice(restaurants)
        base = now - timedelta(minutes=random.randint(0, 60 * 24 * 7))
        order = {"order_id": f"ORD-{i:07d}", "restaurant_id": r["restaurant_id"],
                 "customer_id": f"C-{random.randint(1, 20000):05d}", "city": r["city"],
                 "amount_mad": round(random.uniform(40, 350), 2),
                 "items_count": random.randint(1, 8),
                 "distance_km": round(random.uniform(0.3, 9), 1),
                 "courier_id": f"L-{r['city'][:3].upper()}-{random.randint(1, 15):02d}"}
        status = random.choices(STATUSES, weights=[0, 0, 90, 10])[0]
        t = base
        if random.random() < 0.05:                       # evenement en retard
            t = base - timedelta(minutes=random.randint(1, 15))
        ev = dict(order, status=status, event_time=t.strftime("%Y-%m-%dT%H:%M:%SZ"))
        events.append(ev)
        if random.random() < 0.02:                       # doublon
            events.append(dict(ev))
    random.shuffle(events)
    with open(f"{args.out}/orders_history.jsonl", "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    print(f"{len(events)} evenements ecrits dans {args.out}/")


if __name__ == "__main__":
    main()
