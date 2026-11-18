# 06. Hướng dẫn dựng & chạy Đợt 1 (Walking Skeleton)

## Bước 1: Chuẩn bị file `.env`
Đi vào thư mục `infra/` và copy file mẫu:

```bash
cd ~/Documents/Explore/DS/flink-chaos-recsys/infra
cp .env.example .env
```

- **Nếu chạy thử tất cả trên máy hiện tại (hoặc tất cả trong 1 LXC trên Dell):**
  - Để `HOST_IP=127.0.0.1` (nếu test từ cùng máy) hoặc `HOST_IP=<IP_LAN_CUA_LXC>` (nếu máy Mac sẽ bắn producer hoặc gọi Redis tới LXC đó).
  - Giữ nguyên `KAFKA_BOOTSTRAP_SERVERS=redpanda:29092` và `REDIS_HOST=redis`.
- **Nếu chạy Mode Tách (Redpanda + Redis trên Dell, Flink trên Mac):**
  - Trên Dell: chạy `docker compose --profile data up -d` (với `HOST_IP=<IP_DELL>`).
  - Trên Mac: sửa `.env` thành `KAFKA_BOOTSTRAP_SERVERS=<IP_DELL>:9092`, `REDIS_HOST=<IP_DELL>`, rồi chạy `docker compose --profile flink up -d --build`.

---

## Bước 2: Build và khởi động cụm
```bash
docker compose --profile all up -d --build
```
*(Lần đầu build image PyFlink mất khoảng 2–4 phút để tải Python và `apache-flink==1.19.1`).*

Kiểm tra các giao diện Web UI:
- **Flink Web UI:** `http://<HOST_IP>:8081`
- **Redpanda Console:** `http://<HOST_IP>:8080`

---

## Bước 3: Tạo Topic & Submit PyFlink Job

1. Tạo topic `user_events` với 4 partitions:
   ```bash
   docker exec -it redpanda rpk topic create user_events -p 4 -r 1
   ```
2. Submit PyFlink Job vào Flink JobManager:
   ```bash
   docker exec -it flink-jobmanager flink run -py /opt/flink/jobs/walking_skeleton.py -d
   ```
   Mở `http://<HOST_IP>:8081` kiểm tra tab **Running Jobs** sẽ thấy job `WalkingSkeletonRecSys` đang chạy xanh kèm sơ đồ DAG.

---

## Bước 4: Chạy Producer và Kiểm tra kết quả trên Redis

1. Cài dependencies Python nhẹ cho Producer (trên máy chạy producer):
   ```bash
   pip install -r requirements.txt
   python src/producer/sample_producer.py --bootstrap <HOST_IP>:9092 --rate 10
   ```
2. Mở terminal khác kiểm tra dữ liệu cập nhật real-time trong Redis:
   ```bash
   docker exec -it redis redis-cli ZREVRANGE trending_products 0 9 WITHSCORES
   docker exec -it redis redis-cli LRANGE recent_views:user_0001 0 4
   ```
