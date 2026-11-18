# Flink Chaos RecSys Arena

Hệ thống thử nghiệm **Real-time Streaming Recommendation** kết hợp **Adaptive Chaos Engineering (Red Team vs. Blue Team GameDay)** chạy trên hạ tầng hybrid: **Proxmox VE Homelab (Dell Inspiron 3537: 16GB RAM, 4 vCPU, 1TB HDD)** và **macOS (Dev & Serving)**.

Dự án này mở rộng bài toán gợi ý sản phẩm từ project nền tảng (`22120210` - Amazon Beauty) từ xử lý batch tĩnh sang xử lý luồng thời gian thực (True Streaming) với Apache Flink, dữ liệu quy mô lớn (50–60 GB), cùng môi trường đối kháng tự động để rèn luyện kỹ năng SRE / DataOps.

---

## Tài liệu thiết kế hệ thống

1. **[01. Kiến trúc hệ thống & Hạ tầng](docs/01-architecture.md)**
   - Sơ đồ luồng dữ liệu End-to-End (Hot Path & Cold Path).
   - Công dụng chi tiết của từng component.
   - Phân bổ tài nguyên trên PVE LXC + Mac và 3 lưu ý kỹ thuật sống còn (HDD I/O, `ADVERTISED_LISTENERS`, Redpanda `--developer-mode`).

2. **[02. Use Case thực chiến & Dữ liệu quy mô lớn](docs/02-usecase-and-data.md)**
   - Lựa chọn dataset 50–60 GB (Amazon Reviews 2023 / REES46).
   - Kịch bản "Đêm Flash Sale 00:00" với các edge case thực tế: Traffic Spike, Hot Key Skew, Out-of-order, Late Data, Duplicate Retry, Bot/Zombie Session, Poison Pills.
   - Nối kết quả real-time vào bài toán RecSys (Negative Filtering, Abandoned Cart Trigger).

3. **[03. Mô hình đối kháng Chaos Arena (Red Team vs. Blue Team)](docs/03-chaos-arena.md)**
   - Cơ chế cách ly ngữ cảnh giữa Chaos Agent (Red Team) và Debug Agent (Blue Team).
   - Blackbox Ledger và cách chấm điểm SLA (MTTD / MTTR).
   - Cơ chế Adaptive Chaos (tự đọc code & bản vá để nâng độ khó).

4. **[04. Danh mục Chaos Control Primitives & Guardrails](docs/04-chaos-primitives.md)**
   - Triết lý thiết kế "Primitives over Scenarios" để người vận hành không biết trước đề thi.
   - Bộ 20+ Primitives chia theo 6 nhóm: Compute, Network, Disk I/O, Stream/Payload, Broker/Redis, Time/Clock.
   - Hàng rào an toàn (Blast Radius Guardrails) bảo vệ PVE Host.

5. **[05. Lộ trình triển khai MVP (4 Đợt)](docs/05-roadmap-mvp.md)**
   - Kế hoạch triển khai từ Walking Skeleton đến Chaos Arena hoàn chỉnh kèm Definition of Done cho từng đợt.
