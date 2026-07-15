"""
Synthetic e-commerce clickstream event generator.

Simulates realistic user behavior events (view, add_to_cart, purchase, remove_from_cart)
with intentional data quality issues (nulls, duplicates, malformed timestamps) so the
downstream PySpark pipeline has real problems to solve -- not a clean toy dataset.

Usage:
    python generate_events.py --num-events 50000 --num-users 2000 --output-dir ../../data/sample
"""
import argparse
import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

EVENT_TYPES = ["view", "add_to_cart", "remove_from_cart", "purchase"]
EVENT_WEIGHTS = [0.70, 0.18, 0.05, 0.07]  # realistic funnel drop-off

CATEGORIES = [
    "electronics", "apparel", "home_kitchen", "books", "sports_outdoors",
    "beauty", "toys", "grocery", "automotive", "office_supplies",
]


def _make_product_catalog(n_products=500):
    catalog = []
    for i in range(n_products):
        catalog.append({
            "product_id": f"P{i:05d}",
            "category": random.choice(CATEGORIES),
            "price": round(random.uniform(4.99, 899.99), 2),
        })
    return catalog


def _random_session_events(user_id, session_start, catalog, max_events=12):
    """Generate a plausible sequence of events for one browsing session."""
    events = []
    ts = session_start
    n_events = random.randint(1, max_events)
    session_id = str(uuid.uuid4())
    cart = []

    for _ in range(n_events):
        product = random.choice(catalog)
        event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

        # purchase only makes sense if something is in cart
        if event_type == "purchase" and not cart:
            event_type = "add_to_cart"
        if event_type == "add_to_cart":
            cart.append(product["product_id"])
        if event_type == "remove_from_cart" and cart:
            cart.pop()

        ts += timedelta(seconds=random.randint(3, 240))

        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "session_id": session_id,
            "event_type": event_type,
            "product_id": product["product_id"],
            "category": product["category"],
            "price": product["price"],
            "event_timestamp": ts.isoformat(),
        }
        events.append(event)

    return events


def _inject_data_quality_issues(events, dirty_rate=0.05):
    """Corrupt a fraction of events to simulate real-world messiness."""
    dirty_events = []
    for e in events:
        e = dict(e)
        r = random.random()
        if r < dirty_rate * 0.3:
            e["user_id"] = None  # missing user
        elif r < dirty_rate * 0.6:
            e["event_timestamp"] = "not-a-timestamp"  # malformed timestamp
        elif r < dirty_rate * 0.8:
            e["price"] = None  # missing price
        elif r < dirty_rate:
            dirty_events.append(dict(e))  # exact duplicate row
        dirty_events.append(e)
    return dirty_events


def generate(num_events, num_users, output_dir, dirty_rate=0.05):
    catalog = _make_product_catalog()
    user_ids = [f"U{i:06d}" for i in range(num_users)]

    all_events = []
    base_date = datetime(2026, 6, 1)

    while len(all_events) < num_events:
        user_id = random.choice(user_ids)
        session_start = base_date + timedelta(
            days=random.randint(0, 29),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )
        all_events.extend(_random_session_events(user_id, session_start, catalog))

    all_events = all_events[:num_events]
    all_events = _inject_data_quality_issues(all_events, dirty_rate=dirty_rate)
    random.shuffle(all_events)

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "clickstream_events.json"

    with open(out_path, "w") as f:
        for e in all_events:
            f.write(json.dumps(e) + "\n")

    print(f"Wrote {len(all_events)} events to {out_path}")
    print(f"  ~{dirty_rate*100:.0f}% of rows contain injected data quality issues")
    return out_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic clickstream data")
    parser.add_argument("--num-events", type=int, default=50000)
    parser.add_argument("--num-users", type=int, default=2000)
    parser.add_argument("--dirty-rate", type=float, default=0.05)
    parser.add_argument("--output-dir", type=str, default="data/sample")
    args = parser.parse_args()

    generate(args.num_events, args.num_users, args.output_dir, args.dirty_rate)
