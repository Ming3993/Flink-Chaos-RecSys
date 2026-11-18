"""
Đợt 1 - Walking Skeleton Producer.
Giả lập luồng sự kiện người dùng (view, cart, purchase) bắn vào Redpanda topic `user_events`.
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer


SAMPLE_USERS = [f"user_{i:04d}" for i in range(1, 101)]
SAMPLE_ASINS = [
    "B001E4KFG0", "B00813GRG4", "B000LQOCH0", "B000UA0QIQ",
    "B006GWO5WK", "B000F83SZQ", "B0051VVOB2", "B0015DYMVO",
    "B003EM8008", "B007WTAJTO", "B004GF8TIK", "B009SYZ8OC",
]
EVENT_WEIGHTS = [("view", 0.75), ("cart", 0.18), ("purchase", 0.07)]


def pick_event_type() -> str:
    r = random.random()
    acc = 0.0
    for ev, w in EVENT_WEIGHTS:
        acc += w
        if r <= acc:
            return ev
    return "view"


def main() -> None:
    parser = argparse.ArgumentParser(description="Walking Skeleton Event Producer")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Redpanda bootstrap server")
    parser.add_argument("--topic", default="user_events", help="Kafka/Redpanda topic")
    parser.add_argument("--rate", type=float, default=10.0, help="Events per second")
    parser.add_argument("--count", type=int, default=0, help="Total events (0 = infinite)")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"[Producer] Bắt đầu bắn tới {args.bootstrap} | topic={args.topic} | rate={args.rate} ev/s")
    interval = 1.0 / max(args.rate, 0.1)
    sent = 0

    try:
        while args.count == 0 or sent < args.count:
            user_id = random.choice(SAMPLE_USERS)
            event = {
                "event_id": str(uuid.uuid4()),
                "user_id": user_id,
                "asin": random.choice(SAMPLE_ASINS),
                "event_type": pick_event_type(),
                "price": round(random.uniform(5.0, 120.0), 2),
                "event_time": int(datetime.now(timezone.utc).timestamp() * 1000),
            }
            producer.send(args.topic, key=user_id, value=event)
            sent += 1

            if sent % 50 == 0:
                producer.flush()
                print(f"[Producer] Đã gửi {sent} events (mẫu gần nhất: {event['user_id']} -> {event['event_type']} {event['asin']})")

            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n[Producer] Đã dừng bởi người dùng.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
