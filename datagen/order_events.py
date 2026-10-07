# Purpose: Generates synthetic e-commerce order events to Kafka for the
# streaming_lakehouse lab.
#
# Simulates a stream of orders moving through their lifecycle:
#   PLACED -> PAID -> SHIPPED -> DELIVERED  (occasionally: PLACED/PAID -> CANCELLED)
#
# A single hardcoded order (order_id = "ORD-TEST") is mixed into the same
# random simulation as every other order, so its statuses land spread out
# over time instead of arriving as a tight back-to-back burst — the only
# difference is it can never be cancelled, so it's guaranteed to eventually
# reach every status in order. A reliable, memorable order_id for live demos
# (see DEMO_ORDER_ID below).
#
# Events are published as JSON to the Kafka topic `orders_log`. A Flink job
# (Lab 1, Step 4) reads this topic and inserts into fluss.orders.orders_log —
# this generator never talks to Fluss directly.
#
# product_id values reference the 500-row product_catalog seeded into
# PostgreSQL by the environment setup (see ../pg_shop_ddl.sql).
# The generator doesn't query Postgres for real prices — amount is a plausible
# random order total, independent of the product's actual catalog price.

import configparser
import json
import os
import random
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer

# ── Configuration ─────────────────────────────────────────────────────────────
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "configuration/configuration.ini"))

bootstrap_servers = config["KAFKA"]["bootstrap_servers"]
topic_orders      = config["KAFKA"]["topic_orders"]

min_freq_ms   = int(config["GENERATOR"]["min_freq_ms"])
max_freq_ms   = int(config["GENERATOR"]["max_freq_ms"])
num_orders    = int(config["GENERATOR"]["num_orders"])
cancel_chance = float(config["GENERATOR"]["cancel_chance"])

# ── Constants ─────────────────────────────────────────────────────────────────
CUSTOMERS = ["CUST-" + str(n) for n in range(1001, 1041)]

# product_ids match the 500 rows seeded into PostgreSQL's product_catalog
# (PRD-00001 .. PRD-00500) — see datagen/data/products.csv
PRODUCT_IDS = ["PRD-{:05d}".format(n) for n in range(1, 501)]

STATUS_FLOW = ["PLACED", "PAID", "SHIPPED", "DELIVERED"]

# A hardcoded order that always runs through every lifecycle status, eventually
# — mixed into the same random advancement as everything else, just immune to
# cancellation. Use this order_id for live demos when you need a guaranteed
# example instead of chasing a random one through the stream.
DEMO_ORDER_ID   = "ORD-TEST"
DEMO_CUSTOMER   = "CUST-DEMO"
DEMO_PRODUCT_ID = "PRD-00001"
DEMO_AMOUNT     = 99.99


# ── Helpers ───────────────────────────────────────────────────────────────────
def new_order_id() -> str:
    return "ORD-" + uuid.uuid4().hex[:8].upper()


def make_event(order_id: str, customer_id: str, product_id: str, status: str, amount: float) -> dict:
    return {
        "order_id":    order_id,
        "customer_id": customer_id,
        "product_id":  product_id,
        "status":      status,
        "amount":      round(amount, 2),
        # matches Flink's default JSON-format TIMESTAMP parsing: "yyyy-MM-dd HH:mm:ss.SSS"
        "event_time":  datetime.now().isoformat(" ", "milliseconds"),
    }


# ── Kafka Producer ────────────────────────────────────────────────────────────
def get_producer():
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def publish(producer, evt: dict) -> None:
    # Keyed by order_id: all of an order's events go to one partition, so they stay in
    # lifecycle order for every consumer.
    producer.send(topic_orders, key=evt["order_id"], value=evt)
    print(
        f"[ORDER] {evt['order_id']} | {evt['customer_id']} "
        f"| {evt['product_id']} | {evt['status']:10s} | ${evt['amount']}"
    )


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    producer = get_producer()
    print(f"[order_events] Producing to Kafka topic '{topic_orders}' at {bootstrap_servers} ...")

    # active_orders: order_id → {customer_id, product_id, amount, status_idx}
    active: dict = {}

    # Place the fixed demo order first, so it's easy to spot as the earliest
    # order in the topic. It then advances through PAID/SHIPPED/DELIVERED at
    # the same random pace as everything else — see the cancellation guard
    # below for how it's kept immune from ever being CANCELLED instead.
    active[DEMO_ORDER_ID] = {
        "customer_id": DEMO_CUSTOMER,
        "product_id":  DEMO_PRODUCT_ID,
        "amount":      DEMO_AMOUNT,
        "status_idx":  0,
    }
    publish(producer, make_event(DEMO_ORDER_ID, DEMO_CUSTOMER, DEMO_PRODUCT_ID, "PLACED", DEMO_AMOUNT))

    # Seed an initial pool of orders spread across lifecycle stages
    for _ in range(num_orders // 2):
        oid      = new_order_id()
        customer = random.choice(CUSTOMERS)
        product  = random.choice(PRODUCT_IDS)
        amount   = random.uniform(8.0, 400.0)
        idx      = random.randint(0, 1)   # start at PLACED or PAID
        active[oid] = {
            "customer_id": customer,
            "product_id":  product,
            "amount":      amount,
            "status_idx":  idx,
        }
        publish(producer, make_event(oid, customer, product, STATUS_FLOW[idx], amount))

    producer.flush()

    print(
        f"\n[order_events] Generator started: {len(active)} initial orders in flight\n"
        f"  Topic : {topic_orders}\n"
        f"  Delay : {min_freq_ms}-{max_freq_ms} ms per event\n"
    )

    while True:
        # ── Maybe place a new order ─────────────────────────────────────────
        # Keeps the pool topped up at num_orders. Each tick advances ONE order, and an
        # order needs ~3 advances to finish, so ~1 order completes every 3 ticks: placing
        # with a higher chance than that (0.5) holds the pool near num_orders. With 200
        # in flight an order lives ~1 minute (median; p90 ~2 minutes), long enough for
        # PLACED/PAID/SHIPPED to be visible in Grafana. (At 0.2 the pool
        # drained to a handful of orders that finished within seconds.)
        if len(active) < num_orders and random.random() < 0.5:
            oid      = new_order_id()
            customer = random.choice(CUSTOMERS)
            product  = random.choice(PRODUCT_IDS)
            amount   = random.uniform(8.0, 400.0)
            active[oid] = {
                "customer_id": customer,
                "product_id":  product,
                "amount":      amount,
                "status_idx":  0,
            }
            publish(producer, make_event(oid, customer, product, "PLACED", amount))

        # ── Advance a random active order ───────────────────────────────────
        if active:
            oid   = random.choice(list(active.keys()))
            order = active[oid]
            idx   = order["status_idx"]

            # Orders can only cancel from PLACED or PAID — except the demo
            # order, which must always make it all the way to DELIVERED
            if idx < 2 and oid != DEMO_ORDER_ID and random.random() < cancel_chance:
                publish(producer, make_event(oid, order["customer_id"], order["product_id"], "CANCELLED", order["amount"]))
                del active[oid]
            elif idx < len(STATUS_FLOW) - 1:
                order["status_idx"] += 1
                next_status          = STATUS_FLOW[order["status_idx"]]
                publish(producer, make_event(oid, order["customer_id"], order["product_id"], next_status, order["amount"]))

                if next_status == "DELIVERED":
                    del active[oid]

        producer.flush()
        time.sleep(random.randint(min_freq_ms, max_freq_ms) / 1000)


if __name__ == "__main__":
    main()
