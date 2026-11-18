---
name: flink-troublemaker
description: >-
  Specialized Red Team skill for designing and executing multi-vector chaos attacks
  against Apache Flink, Redpanda, Redis, and FastAPI streaming pipelines using the
  21 Chaos Control Primitives, Blackbox Ledger logging, and Adaptive Chaos tactics.
---

# Flink Troublemaker (Red Team Playbook)

Use this skill whenever designing an attack, probing pipeline code for hidden assumptions, executing Chaos Control Primitives, or adapting after Blue Team applies a patch.

## 1. Recon Phase: Soi điểm yếu trong Code Flink / Pipeline
Trước khi ra đòn, hãy đọc source code của pipeline và tìm các lỗi/giả định kinh điển:
- **Watermark & Event Time:** Code có chặn timestamp nhảy tới tương lai (future-dated events) không? Nếu dùng `forBoundedOutOfOrderness` thô, chỉ cần 1 event năm 2099 là đủ gây **Watermark Poisoning** làm drop toàn bộ dữ liệu thật.
- **State Growth & OOM:** `ListState` / `MapState` có `StateTtlConfig` hoặc giới hạn kích thước (Ring Buffer) không? Nếu không, chỉ cần giả lập 1 con Bot mở session không ngừng nghỉ (Zombie Session) là TaskManager sẽ chết OOM.
- **Key Skew (`keyBy`):** Có dùng Salting / Two-phase Aggregation không? Nếu không, dùng `stream_skew_distribution` dồn 70% tải vào 1 `product_id` sẽ làm nghẽn chết 1 Task Slot duy nhất.
- **Error Handling & Schema Drift:** JSON parser có bọc `try/except` đẩy sang DLQ không? Các bước transform phía sau có xử lý `price = "free"`, `NaN`, `null`, hoặc số âm không?
- **Checkpoint & HDD I/O:** Checkpoint interval và timeout đặt thế nào? Kết hợp `disk_io_throttle` lúc Flink đang Checkpoint trên HDD 1TB để gây Checkpoint Timeout Cascade.

## 2. Bộ 21 Chaos Primitives (Tham chiếu `../docs/04-chaos-primitives.md`)
- **Compute:** `kill_process`, `pause_container` (GC pause), `throttle_cpu`, `stress_memory`, `restart_storm`
- **Network:** `net_delay`, `net_packet_loss`, `net_throttle_bandwidth`, `net_partition`, `net_corrupt_or_duplicate`
- **Storage/HDD:** `disk_io_throttle`, `disk_stress_iops`, `disk_fill_sandbox`
- **Stream/Payload:** `stream_rate_control`, `stream_skew_distribution`, `stream_inject_raw_payload`, `stream_set_dynamic_mutator`, `stream_time_warp`
- **Broker/Cache/Clock:** `redis_client_pause`, `redis_evict_keys`, `clock_skew`

## 3. Quy trình thực thi một đòn tấn công (Execution Protocol)
1. **Ghi Blackbox trước:** Append một dòng JSON vào `./memory/blackbox_ledger.jsonl` gồm:
   `{"incident_id": "INC-XXX", "start_ts": "<ISO8601>", "hypothesis": "...", "primitives": [...], "status": "ACTIVE"}`
2. **Thực thi phối hợp (Multi-Vector):** Ưu tiên phối hợp 2–3 primitives gây nhiễu triệu chứng (ví dụ: vừa `net_throttle_bandwidth` đường Flink->Redis vừa `stream_rate_control` tăng 10x để Blue Team khó phân biệt lỗi do mạng hay do tải).
3. **Hậu kiểm (Adaptive Learning):** Khi Blue Team báo xong và mở Blackbox chấm MTTD/MTTR, cập nhật `status: "RESOLVED"`, đọc bản vá của Blue Team, và ghi bài học + phương án phản công vào `./memory/attack_history.md`.
