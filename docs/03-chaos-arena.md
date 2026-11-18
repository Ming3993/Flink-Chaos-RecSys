# 03. Mô hình đối kháng Chaos Arena (Red Team vs. Blue Team)

## 1. Mục tiêu thiết kế
Tránh tình trạng "tự ra đề, tự đi giải" khi kiểm thử hệ thống. Hệ thống vận hành theo mô hình **GameDay đối kháng tự động** với hai vai trò tách biệt hoàn toàn:

- **Red Team (Chaos Agent):** Tự động quan sát code, tìm điểm yếu và kích hoạt các sự cố bất ngờ không báo trước thông qua bộ Chaos Control Primitives.
- **Blue Team (Người vận hành + Debug Agent):** Hoàn toàn không biết kịch bản hay thời điểm tấn công. Phải dựa vào Observability (metrics, logs, alerts) để phát hiện, tìm nguyên nhân gốc (RCA) và khắc phục sự cố trong giới hạn SLA.

---

## 2. Nguyên tắc cách ly ngữ cảnh (Context Isolation)

```text
  [Source Code Pipeline] ──(read)──► [Chaos Agent (Red Team)]
                                            │
                       ┌────────────────────┴────────────────────┐
                       ▼                                         ▼
           [Chaos Control API]                       [blackbox_ledger.jsonl]
           (Thực thi Primitives)                     (Ghi nhận sự cố & Start TS)
                       │                                         ▲
                       ▼                                         │ (Chỉ mở đọc
             [Hệ thống Streaming]                                │  sau khi đóng
                       │                                         │  Incident)
                       ▼                                         │
             [Observability Stack] ──► [Blue Team: Bạn + Debug Agent]
```

- **Quy tắc bất khả xâm phạm:**
  1. Trong lúc sự cố đang diễn ra, Debug Agent và người dùng **tuyệt đối không được phép truy cập** file `blackbox_ledger.jsonl` hoặc logs nội bộ của Chaos Agent.
  2. Blue Team chỉ được đọc: Prometheus/Grafana metrics, Flink REST API, Redpanda lag, Redis stats, container logs và source code pipeline.

---

## 3. Đo lường SLA (MTTD & MTTR)

Mỗi lần Chaos Agent ra tay sẽ sinh một `incident_id` và ghi `T_start` vào `blackbox_ledger.jsonl`. Khi Blue Team xác nhận hệ thống đã ổn định trở lại (`T_resolved`), hai bên đối chiếu:

- **MTTD (Mean Time To Detect):** $T_{\text{detected}} - T_{\text{start}}$
  Đo độ nhạy của hệ thống giám sát và cảnh báo.
- **MTTR (Mean Time To Resolve):** $T_{\text{resolved}} - T_{\text{start}}$
  Đo tốc độ khoanh vùng lỗi, tìm nguyên nhân gốc và vá hệ thống.

### Ngưỡng SLA tham khảo
| Mức độ | Mô tả triệu chứng | Ngưỡng MTTD | Ngưỡng MTTR |
| :--- | :--- | :--- | :--- |
| **Sev-1 (Critical)** | Flink Job sập/Restart loop, FastAPI trả lỗi 5xx, mất kết nối Redis/Redpanda. | < 30 giây | < 3 phút |
| **Sev-2 (Degraded)** | Consumer Lag tăng liên tục, Backpressure đỏ, p99 Latency vượt ngưỡng (> 100ms). | < 1 phút | < 10 phút |
| **Sev-3 (Silent Corruption)** | Job vẫn chạy xanh nhưng dữ liệu bị drop ngầm (Watermark poisoning) hoặc sai lệch số liệu (Duplicate). | < 3 phút | < 15 phút |

---

## 4. Cơ chế Adaptive Chaos (Tự thích nghi sau mỗi vòng)

Sau khi một vòng GameDay kết thúc và incident được đóng, Chaos Agent được cấp quyền đọc `git diff` hoặc các thay đổi cấu hình mà Blue Team vừa áp dụng để **học hỏi và thiết kế đòn đánh kế tiếp**:

- **Ví dụ 1 (Từ Poison Pill đến Schema Drift):**
  - Vòng 1: Chaos Agent thả JSON cụt đuôi làm crash job $\rightarrow$ Blue Team thêm `try/except` đẩy sang DLQ.
  - Vòng 2: Chaos Agent không thả JSON lỗi cú pháp nữa, mà giữ nguyên JSON hợp lệ nhưng đổi kiểu trường `price` từ `float` sang `"free"` hoặc truyền `NaN`/`Infinity` để đánh vào tầng tính toán phía sau.
- **Ví dụ 2 (Từ Hot Key đến Watermark Poisoning):**
  - Vòng 1: Chaos Agent dồn traffic vào 1 `product_id` $\rightarrow$ Blue Team thêm Salting key.
  - Vòng 2: Chaos Agent trộn 1 event có `timestamp = năm 2099` vào luồng sạch. Nếu Flink dùng `forBoundedOutOfOrderness` thông thường, Watermark nhảy vọt lên năm 2099, khiến **toàn bộ dữ liệu thật phía sau bị coi là quá hạn và bị drop sạch** trong khi Job vẫn báo trạng thái `RUNNING`.
- **Ví dụ 3 (Khai thác Trade-off):**
  - Nếu thấy Blue Team tăng `checkpoint interval` lên quá dài (ví dụ 5 phút) để giảm tải cho HDD 1TB, Chaos Agent canh đúng phút thứ 4:50 để `SIGKILL` TaskManager, khiến lượng dữ liệu phải replay và thời gian phục hồi bị kéo dài tối đa.
