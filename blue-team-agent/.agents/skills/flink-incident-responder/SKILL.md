---
name: flink-incident-responder
description: >-
  Structured Blue Team SRE incident response runbook for Apache Flink, Redpanda,
  Redis, and FastAPI pipelines. Enforces MTTD/MTTR SLA tracking, systematic triage,
  and post-mortem documentation without peeking at Red Team blackbox logs.
---

# Flink Incident Responder (Blue Team Runbook)

Use this skill whenever an alert fires, metrics degrade, or the user asks to investigate and debug an incident in the streaming pipeline.

## 1. Triage Checklist (Định vị nhanh trong 60 giây)
Kiểm tra theo thứ tự từ Downstream ngược về Upstream để không bị đánh lừa bởi triệu chứng phụ:
1. **Serving Layer (FastAPI):** Kiểm tra HTTP 5xx, p99 latency, và kết nối tới Redis.
2. **Feature Store (Redis):** Kiểm tra `redis-cli INFO`, latency (`--latency`), blocked clients, bộ nhớ (`used_memory`), hoặc `CLIENT PAUSE`.
3. **Stream Engine (Apache Flink):**
   - Kiểm tra trạng thái Job qua REST API `/jobs/overview` (có đang `RESTARTING` hay `FAILING` không?).
   - Kiểm tra **Backpressure** và **Busy Time** từng operator (để phân biệt nghẽn tại Sink hay nghẽn tại `keyBy` skew).
   - Kiểm tra **Watermark** (Watermark có bị nhảy cóc lên năm tương lai khiến `numRecordsOut` = 0 không?).
   - Kiểm tra **Checkpoint** (duration tăng vọt hay timeout do HDD I/O?).
4. **Broker (Redpanda):** Kiểm tra Consumer Lag, disk space, và throughput đầu vào.
5. **Infra (LXC / Network / Disk):** Kiểm tra CPU throttling, RAM/OOM (`dmesg`), Disk I/O wait (`iostat`), và Network latency/packet loss giữa các LXC.

## 3. Tiêu chuẩn đóng Incident (Resolution Criteria)
Chỉ tuyên bố sự cố kết thúc (`T_resolved`) khi đạt đủ 4 điều kiện:
- Flink Job ở trạng thái `RUNNING` ổn định, không restart lại trong ít nhất 60 giây.
- Redpanda Consumer Lag đang giảm nhanh hoặc đã về mức cân bằng (`~0`).
- DLQ error rate và drop rate trở về mức bình thường.
- FastAPI p99 latency dưới ngưỡng SLA (< 50ms).

## 4. Ghi Post-Mortem vào `./memory/post_mortems/`
Sau khi đối chiếu kết quả với người dùng, tạo file `./memory/post_mortems/INC-XXX.md` ghi lại:
- Thời gian `T_start`, `T_detected` (MTTD), `T_resolved` (MTTR) và kết quả Đạt/Trượt SLA.
- Nguyên nhân gốc (Root Cause) và bài học cập nhật vào `./memory/runbooks.md`.
