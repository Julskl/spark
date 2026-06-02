import os
import time
import itertools
import numpy as np
import clickhouse_connect

#Каждые BATCH_INTERVAL секунд генерирует пачку заказов и пишет их в delivery.orders 
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
BATCH_INTERVAL = float(os.getenv("BATCH_INTERVAL", "5"))

# Истинные коэффициенты модели
W_DISTANCE = 4.0
W_STOPS = 6.0 
W_HOUR = 0.5  
INTERCEPT = 5.0   


def get_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD
    )


def wait_for_clickhouse(retries=60, delay=2.0):
    for attempt in range(1, retries + 1):
        try:
            client = get_client()
            client.command("SELECT 1")
            print(f"[generator] ClickHouse доступен (попытка {attempt})", flush=True)
            return client
        except Exception as exc:
            print(f"[generator] ждём ClickHouse ({attempt}/{retries}): {exc}", flush=True)
            time.sleep(delay)
    raise RuntimeError("ClickHouse так и не поднялся")


def main():
    client = wait_for_clickhouse()
    rng = np.random.default_rng(42)
    counter = itertools.count(1)

    print("[generator] старт генерации заказов на доставку", flush=True)
    while True:
        distance = rng.uniform(1.0, 20.0, size=BATCH_SIZE)        # км
        stops = rng.integers(1, 9, size=BATCH_SIZE)               # 1..8 точек
        hour = rng.integers(0, 24, size=BATCH_SIZE)               # 0..23
        noise = rng.normal(0.0, 2.0, size=BATCH_SIZE)             # шум, мин

        delivery_time = (
            INTERCEPT
            + W_DISTANCE * distance
            + W_STOPS * stops
            + W_HOUR * hour
            + noise
        )

        rows = []
        for i in range(BATCH_SIZE):
            rows.append([
                int(next(counter)),
                float(distance[i]),
                int(stops[i]),
                int(hour[i]),
                float(delivery_time[i]),
            ])

        client.insert(
            "delivery.orders",
            rows,
            column_names=["order_id", "distance_km", "num_stops",
                          "hour_of_day", "delivery_time_min"],
        )
        total = client.command("SELECT count() FROM delivery.orders")
        print(f"[generator] вставлено {BATCH_SIZE} заказов, всего: {total}", flush=True)
        time.sleep(BATCH_INTERVAL)


if __name__ == "__main__":
    main()
