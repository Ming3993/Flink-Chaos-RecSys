# 01. Kiến trúc hệ thống & Phân bổ hạ tầng

## 1. Sơ đồ luồng dữ liệu tổng thể

```text
[1. Dataset 50-60GB] ──► [2. Chaos Producer] ──► [3. Redpanda (Broker)] ──► [4. Apache Flink]
   (HDD 1TB - PVE)         (Python - PVE)            (PVE - LXC 1)            (PVE - LXC 2)
                                                                                 │       │
                                            ┌────────────────────────────────────┘       └─────────────────┐
                                            ▼                                                              ▼
                                [5. Redis (Feature Store)]                                   [6. Cold Storage / DLQ]
                                       (PVE - LXC 1)                                              (HDD 1TB - PVE)
                                            │
                                            ▼
                                [7. FastAPI Serving Layer] ◄─── (Offline Model .npz từ project 22120210)
                                       (Mac / PVE)

       ═══════════════ [8. Observability: Flink UI + Prometheus/Grafana + Logs] ═══════════════
```

---

## 2. Công dụng của từng Component

| Thành phần | Vị trí triển khai | Vai trò & Công dụng chính |
| :--- | :--- | :--- |
| **Dataset Store** | PVE (1TB HDD) | Lưu trữ bộ dữ liệu gốc dung lượng 50–60 GB (Amazon Reviews 2023 hoặc REES46) làm nguồn phát sự kiện, tránh làm đầy SSD của máy Mac. |
| **Chaos Producer** | PVE (LXC 1) | Đọc file cục bộ trên HDD, giả lập hành vi người dùng và bơm các yếu tố nhiễu thực tế (traffic spike, late data, duplicate, bot, poison pills) vào Broker. |
| **Redpanda** | PVE (LXC 1) | Message Broker tương thích giao thức Kafka nhưng nhẹ hơn (không cần JVM/Zookeeper). Đóng vai trò bộ đệm (buffer) chịu tải cao, tách rời tốc độ bắn của Producer và tốc độ xử lý của Flink, cho phép replay dữ liệu khi Flink restart. |
| **Apache Flink** | PVE (LXC 2) | Trái tim xử lý luồng (True Streaming). Thực hiện làm sạch, lọc trùng (Deduplication), cô lập lỗi sang DLQ, xử lý Event Time & Watermark, chia tải Hot Key (Salting), phát hiện Bot (CEP), bắt bỏ giỏ hàng (Timer), và duy trì Checkpoint. |
| **Redis** | PVE (LXC 1) | Online Feature Store độ trễ dưới 1ms. Lưu N sản phẩm user vừa xem, danh sách sản phẩm vừa mua để loại trừ (Negative Filtering), bảng xếp hạng trending real-time, và danh sách Bot bị chặn. |
| **Cold Storage & DLQ** | PVE (1TB HDD) | Lưu các bản ghi lỗi định dạng (Dead Letter Queue), bản ghi quá trễ (Side Output) để hậu kiểm, và dữ liệu tương tác sạch dạng Parquet phục vụ train lại model offline định kỳ. |
| **FastAPI Serving** | macOS (hoặc PVE) | Tầng phục vụ API (`/recommend`, `/next-item`). Kết hợp kết quả từ model offline (`22120210`) với ngữ cảnh thời gian thực đọc từ Redis qua LAN. |
| **Observability Stack** | PVE / macOS | Thu thập metrics sống (Consumer Lag, Backpressure, Checkpoint Duration, Error Rate, API p99 Latency) qua Flink REST API, Prometheus, Grafana. Là nguồn thông tin duy nhất cho Blue Team phát hiện sự cố. |

---

## 3. Phân bổ tài nguyên trên Proxmox VE (Dell Inspiron 3537)

Cấu hình phần cứng khả dụng trên PVE: **16GB RAM** (còn trống ~12–13GB), **4 vCPU**, **1TB HDD**.

- **LXC 1 (`data-broker`, ~4GB RAM, 300GB disk):**
  - Chứa Dataset 60GB, Chaos Producer, Redpanda, Redis và thư mục Cold Storage / DLQ.
- **LXC 2 (`flink-cluster`, ~6GB RAM, 100GB disk):**
  - Chứa Flink JobManager và TaskManager.
  - Từ máy Mac chỉ cần code logic nhẹ, test trên sample nhỏ, sau đó submit job trực tiếp lên JobManager trên PVE qua REST API / CLI (`flink run -m <IP_PVE>:8081 ...`).
- **Phần dư (~2–3GB RAM):** Dự phòng cho Observability (Prometheus/Grafana) hoặc Chaos Control Plane.

---

## 4. Ba lưu ý kỹ thuật sống còn khi triển khai

1. **Bẫy Random I/O trên HDD 1TB:**
   - Redpanda ghi log tuần tự (sequential write) nên chạy trên HDD ổn, nhưng **RocksDB State Backend** của Flink khi compaction hoặc checkpoint lại tốn rất nhiều random I/O, dễ gây nghẽn disk 100% và kéo theo Backpressure.
   - *Giải pháp:* Với các cửa sổ thời gian ngắn (vài phút đến 1 giờ) kèm `StateTtlConfig` (tự xóa state user cũ), kích thước state thường nằm trong khoảng vài trăm MB – vài GB. Nên ưu tiên dùng **HashMapStateBackend** (lưu trên RAM của LXC 2) để đạt tốc độ cao và giảm tải cho HDD.

2. **Bẫy `ADVERTISED_LISTENERS` qua mạng LAN:**
   - Khi Redpanda chạy trên LXC khác máy với Producer hoặc Flink/Mac, nếu không cấu hình địa chỉ advertise là **IP LAN thực của LXC 1** (ví dụ `192.168.1.x:9092`), client khi kết nối vào sẽ nhận được metadata trả về `localhost:9092` và bị lỗi timeout.

3. **Bẫy Redpanda Hardware Check trên HDD:**
   - Redpanda mặc định tối ưu khắt khe cho NVMe SSD (Direct I/O, `io_uring`). Khi chạy trên HDD hoặc trong môi trường LXC, cần bật chế độ `--developer-mode` (hoặc tắt strict hardware check trong `redpanda.yaml`) để tránh lỗi khởi động hoặc sụt hiệu năng bất thường.
