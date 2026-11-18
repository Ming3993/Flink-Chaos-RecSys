# 05. Lộ trình triển khai MVP (4 Đợt)

Để tránh ôm đồm quá nhiều thành phần cùng lúc, dự án được chia thành 4 đợt triển khai tuần tự theo nguyên tắc: **Thông luồng $\rightarrow$ Chịu tải $\rightarrow$ Lắp giám sát $\rightarrow$ Đấu đối kháng**.

---

## Đợt 1: Walking Skeleton (Thông luồng End-to-End)
**Mục tiêu:** Chứng minh hạ tầng PVE + macOS giao tiếp ổn định trên tập mẫu nhỏ (~50,000 dòng sạch).

- **Các bước thực hiện:**
  1. Khởi tạo `LXC 1` (Redpanda với `--developer-mode` + Redis) và `LXC 2` (Flink JobManager + TaskManager). Cấu hình `ADVERTISED_LISTENERS` theo IP LAN của LXC 1.
  2. Viết Producer Python cơ bản đọc file mẫu trên PVE và bắn vào Redpanda.
  3. Viết Flink Job tối giản: đọc stream $\rightarrow$ lưu 5 sản phẩm xem gần nhất của user vào Redis + đếm Top Trending theo cửa sổ 1 phút.
  4. Kết nối FastAPI (`22120210`) tới Redis trên PVE để đọc feature real-time.
- **Definition of Done (DoD):** Bắn 1 event từ Producer $\rightarrow$ sau 1–2 giây gọi API FastAPI thấy kết quả gợi ý cập nhật tương ứng.

---

## Đợt 2: Scale & Resilience (Dữ liệu 50–60GB & Gia cố Flink)
**Mục tiêu:** Đưa hệ thống lên quy mô tải thật và trang bị cơ chế phòng thủ cho Flink trước dữ liệu bẩn.

- **Các bước thực hiện:**
  1. Tải bộ dữ liệu lớn (**Amazon Reviews 2023** hoặc **REES46**, ~50–60 GB) vào ổ 1TB HDD trên PVE.
  2. Nâng cấp Producer hỗ trợ mô phỏng Flash Sale Spike và trộn dữ liệu nhiễu (late data, duplicate, bot, poison pills).
  3. Gia cố Flink Job:
     - Bật **Event Time + Watermark** và tách **Side Output / DLQ** cho bản ghi lỗi hoặc quá hạn.
     - Triển khai **Deduplication** (`State` + `StateTtlConfig`) và **Two-Phase Aggregation (Salting Key)** cho Hot Key.
     - Bật **Checkpointing** (ưu tiên `HashMapStateBackend` kết hợp TTL để tránh nghẽn random I/O của HDD).
     - Bổ sung logic **Negative Filtering** (loại trừ món vừa mua) và **Abandoned Cart Timer** (`OnTimer` / CEP).
- **Definition of Done (DoD):** Producer bắn > 20,000 events/s kèm 10% nhiễu mà job không crash; giết nóng TaskManager rồi bật lại vẫn tự phục hồi State từ Checkpoint.

---

## Đợt 3: Observability & SLA Baseline (Lắp mắt quan sát cho Blue Team)
**Mục tiêu:** Xây dựng hệ thống đo đạc để Blue Team có thể tự phát hiện bất thường mà không cần nhìn Blackbox.

- **Các bước thực hiện:**
  1. Dựng **Prometheus + Grafana** (hoặc bộ collector đọc Flink REST API `/jobs`, Redpanda `/metrics`, Redis `INFO`).
  2. Thiết kế dashboard theo dõi các chỉ số cốt lõi: *Consumer Lag, Backpressure, Checkpoint Duration, DLQ Rate, CPU/RAM/Disk I/O của LXC, FastAPI p99 Latency*.
  3. Thiết lập ngưỡng SLA (Sev-1, Sev-2, Sev-3) và cơ chế cảnh báo cơ bản khi vượt ngưỡng.
- **Definition of Done (DoD):** Nhìn thấy đầy đủ biến động hệ thống theo thời gian thực trên dashboard và nhận được cảnh báo khi thử nghiệm ngắt kết nối một thành phần.

---

## Đợt 4: Chaos Arena (Kích hoạt Red Team vs. Blue Team)
**Mục tiêu:** Đưa vào vận hành mô hình đấu đối kháng và chấm điểm SLA.

- **Các bước thực hiện:**
  1. Xây dựng **Chaos Control API** (triển khai bộ Primitives trong `docs/04-chaos-primitives.md` kèm Guardrails và Auto-Rollback).
  2. Thiết lập **Blackbox Logger** (`blackbox_ledger.jsonl`) cách ly quyền truy cập với Blue Team.
  3. Kết nối **Chaos Agent (Red Team)** và **Debug Agent (Blue Team)** trên hai ngữ cảnh độc lập.
  4. Chạy GameDay đầu tiên: Chaos Agent tấn công bất ngờ $\rightarrow$ Blue Team debug và vá lỗi $\rightarrow$ Đối chiếu MTTD/MTTR với Blackbox $\rightarrow$ Cho Chaos Agent đọc `git diff` để chuẩn bị vòng tiếp theo.
- **Definition of Done (DoD):** Hoàn thành trọn vẹn ít nhất 1 chu kỳ tấn công bất ngờ – xử lý sự cố – chấm điểm SLA – thích nghi thành công.
