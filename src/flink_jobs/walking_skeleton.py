"""
Đợt 1 - Walking Skeleton PyFlink Job.
Đọc luồng sự kiện từ Redpanda (`user_events`), lưu:
1. Top 5 sản phẩm user vừa xem gần nhất vào Redis (`recent_views:<user_id>`).
2. Danh sách sản phẩm đã mua để Negative Filtering (`purchased:<user_id>`).
3. Bảng xếp hạng sản phẩm tương tác real-time (`trending_products` Sorted Set).
"""

import json
import os
import redis
from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import MapFunction, RuntimeContext


class RedisFeatureUpdater(MapFunction):
    """Cập nhật real-time features của từng user và sản phẩm vào Redis."""

    def __init__(self, redis_host: str, redis_port: int):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.client = None

    def open(self, runtime_context: RuntimeContext):
        self.client = redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            decode_responses=True,
            socket_timeout=2.0,
        )

    def map(self, raw_value: str) -> str:
        event = json.loads(raw_value)
        user_id = event["user_id"]
        asin = event["asin"]
        event_type = event["event_type"]

        pipe = self.client.pipeline()

        # 1. Cập nhật 5 sản phẩm gần nhất user tương tác
        recent_key = f"recent_views:{user_id}"
        pipe.lrem(recent_key, 0, asin)
        pipe.lpush(recent_key, asin)
        pipe.ltrim(recent_key, 0, 4)
        pipe.expire(recent_key, 86400)

        # 2. Negative Filtering nếu user đã mua sản phẩm
        if event_type == "purchase":
            pipe.sadd(f"purchased:{user_id}", asin)

        # 3. Cộng điểm Trending (view=1, cart=3, purchase=5)
        weight = {"view": 1.0, "cart": 3.0, "purchase": 5.0}.get(event_type, 1.0)
        pipe.zincrby("trending_products", weight, asin)

        pipe.execute()
        return f"OK:{user_id}:{event_type}:{asin}"


def main():
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_topics("user_events")
        .set_group_id("flink-walking-skeleton")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "RedpandaUserEventsSource",
    )

    (
        stream.key_by(lambda raw: json.loads(raw)["user_id"])
        .map(RedisFeatureUpdater(redis_host, redis_port), output_type=Types.STRING())
        .name("UpdateRedisFeatures")
    )

    env.execute("WalkingSkeletonRecSys")


if __name__ == "__main__":
    main()
