# 04. Danh mục Chaos Control Primitives & Guardrails

## 1. Triết lý "Primitives over Scenarios"

Để người vận hành **không thể biết trước đề thi**, Chaos Control API **không cung cấp các hàm kịch bản có sẵn** (như `trigger_watermark_poisoning()` hay `trigger_hotkey_skew()`).

Thay vào đó, API chỉ cung cấp một tập hợp đa dạng các **công cụ vật lý và dữ liệu cấp thấp (Primitives)**. Nhiệm vụ của Chaos Agent (LLM) là tự đọc source code của bạn, tự suy luận điểm yếu, rồi **tự phối hợp nhiều Primitives thành một chuỗi tấn công bất ngờ (Multi-vector Attack)**.

---

## 2. Bộ Chaos Control Primitives (21 Methods thuộc 6 nhóm)

### Nhóm 1: Compute & Process Primitives (Đánh vào LXC / Container / JVM)
1. `kill_process(target, signal="SIGKILL" | "SIGTERM")`
   Giết nóng hoặc tắt êm tiến trình mục tiêu (`taskmanager`, `jobmanager`, `redpanda`, `redis`, `fastapi`).
2. `pause_container(target, duration_sec)`
   Đóng băng tiến trình (`SIGSTOP` / `docker pause`) trong vài giây để giả lập **JVM Stop-the-World GC Pause** hoặc treo máy cục bộ (healthcheck không chết hẳn nhưng gây timeout dây chuyền).
3. `throttle_cpu(target, quota_pct, duration_sec)`
   Giới hạn CPU quota (qua cgroups) xuống 10–30% để giả lập tình trạng CPU starvation / noisy neighbor.
4. `stress_memory(target, size_mb, duration_sec)`
   Chiếm dụng RAM tạm thời trong container mục tiêu để ép swap hoặc kích hoạt OOM Killer.
5. `restart_storm(target, count, interval_sec)`
   Tắt bật liên tiếp một worker để gây biến động topology và checkpoint churn.

### Nhóm 2: Network & Traffic Primitives (Dùng `tc netem` / `iptables` trong scope LXC)
6. `net_delay(src_target, dst_port, delay_ms, jitter_ms, distribution="pareto" | "normal")`
   Tạo độ trễ mạng có biến thiên (jitter) giữa hai thành phần (ví dụ giữa Flink và Redis).
7. `net_packet_loss(target, dst_port, loss_pct, correlation_pct)`
   Gây rớt gói tin ngẫu nhiên hoặc theo cụm (burst packet loss).
8. `net_throttle_bandwidth(target, rate_kbps, duration_sec)`
   Bóp băng thông đường truyền (ví dụ bóp xuống 500 kbps) để tạo nghẽn cổ chai ở sink và đẩy Backpressure ngược lên Flink.
9. `net_partition(target_a, target_b, duration_sec)`
   Cô lập mạng (Split-brain / Network Partition) giữa hai service trong thời gian định sẵn.
10. `net_corrupt_or_duplicate(target, corrupt_pct, dup_pct, duration_sec)`
    Gây lỗi checksum hoặc nhân bản gói tin ở tầng mạng.

### Nhóm 3: Storage & Disk I/O Primitives (Đánh vào đặc thù HDD 1TB)
11. `disk_io_throttle(target, read_bps, write_bps, duration_sec)`
    Giới hạn tốc độ đọc/ghi đĩa của container để giả lập nghẽn HDD khi Flink ghi Checkpoint hoặc RocksDB compaction.
12. `disk_stress_iops(target, workers, mode="randwrite" | "seqwrite", duration_sec)`
    Chạy tiến trình đọc/ghi nhiễu vào thư mục sandbox để chiếm dụng IOPS của ổ đĩa cơ.
13. `disk_fill_sandbox(target, size_mb)`
    Tạo file dung lượng lớn trong thư mục tạm/checkpoint của container (có giới hạn trần an toàn) để kích hoạt lỗi hết dung lượng cục bộ (`No space left on device`).

### Nhóm 4: Stream & Payload Mutation Primitives (Tác động vào Chaos Producer)
14. `stream_rate_control(multiplier, pattern="step" | "spike" | "sine", duration_sec)`
    Điều chỉnh tốc độ bắn dữ liệu (tăng đột biến 15x giả lập Flash Sale hoặc sụt về 0 giả lập mất kết nối đầu nguồn).
15. `stream_skew_distribution(zipf_alpha, hot_keys, concentration_pct, duration_sec)`
    Bẻ cong phân phối key, dồn tỷ lệ traffic lớn vào một tập nhỏ các `product_id` hoặc `user_id` để gây lệch tải `keyBy`.
16. `stream_inject_raw_payload(records_list, rate_per_sec)`
    Cho phép Chaos Agent tự sáng tác và bơm thẳng danh sách payload thô (JSON lỗi cú pháp, bytes rác, bản ghi siêu lớn 1MB, schema drift) vào Redpanda.
17. `stream_set_dynamic_mutator(mutator_rule, sample_rate, duration_sec)`
    Gắn bộ biến đổi động lên các event đang chảy (đổi kiểu dữ liệu, xóa trường bắt buộc, gán giá trị biên `NaN`/`-999`, nhân bản `event_id` gây duplicate).
18. `stream_time_warp(offset_sec, sample_rate, mode="lag" | "future_jump", duration_sec)`
    Thay đổi `event_time` của một tỷ lệ bản ghi: đẩy lùi về quá khứ (gây Out-of-order / Late Data) hoặc đẩy vọt tới tương lai (gây **Watermark Poisoning**).

### Nhóm 5: Broker, Cache & Clock Primitives
19. `redis_client_pause(duration_ms)`
    Gọi lệnh `CLIENT PAUSE` của Redis để làm đóng băng toàn bộ phản hồi từ Redis trong vài giây (giả lập Redis bị block bởi lệnh nặng `KEYS *` hoặc quá trình `BGSAVE` fork).
20. `redis_evict_keys(pattern, sample_ratio)`
    Xóa ngẫu nhiên một phần cache/feature trên Redis để giả lập Cold Cache / Cache Eviction đột ngột khi đầy RAM.
21. `clock_skew(target_process, offset_sec, duration_sec)`
    Sử dụng `libfaketime` làm lệch đồng hồ hệ thống cục bộ của một tiến trình so với phần còn lại (giả lập lỗi đồng bộ NTP trong hệ thống phân tán).

---

## 3. Hàng rào an toàn (Blast Radius Guardrails)

Vì Chaos Agent được phép hành động tự do và bất ngờ, **tuyệt đối không cấp quyền SSH root hoặc shell tự do trên Proxmox Host**. Chaos Control API phải đóng vai trò lớp kiểm duyệt với các giới hạn cứng:

1. **Whitelist Targets:** Mọi primitive chỉ được phép nhắm vào đúng danh sách container/LXC của project (`redpanda`, `redis`, `flink-jobmanager`, `flink-taskmanager`, `chaos-producer`, `fastapi`). Cấm tác động lên PVE Host hoặc các LXC khác.
2. **TTL & Auto-Rollback bắt buộc:** Mọi hành động làm suy giảm hệ thống (`net_delay`, `net_partition`, `throttle_cpu`, `disk_io_throttle`, `stream_set_dynamic_mutator`) bắt buộc phải có tham số `duration_sec` tối đa (ví dụ trần `max = 300s`). Hết giờ API tự động hoàn nguyên cấu hình cũ, tránh treo vĩnh viễn.
3. **Storage Cap:** `disk_fill_sandbox` bị khóa trần dung lượng tối đa (ví dụ chỉ được chiếm tối đa 80% dung lượng phân vùng dành cho sandbox, không bao giờ làm đầy toàn bộ ổ 1TB).
4. **Immutable Blackbox Logging:** Mọi request đi qua Chaos Control API đều tự động bị ghi vào `blackbox_ledger.jsonl` trước khi thực thi (kèm `incident_id`, `timestamp`, `method`, `params`).
