# 02. Use Case thực chiến & Dữ liệu quy mô lớn

## 1. Lựa chọn Dataset (Quy mô 50–60 GB)

### Phương án A: Amazon Reviews 2023 (McAuley Lab - UCSD) — Khuyên dùng
- **Quy mô toàn bộ:** ~571.5 triệu reviews trên 33 ngành hàng (~275 GB core review files trên Hugging Face).
- **Cách lấy đúng 50–60 GB:** Chọn tải 3–5 file `.jsonl` của các ngành lớn nhất (*Books*, *Home_and_Kitchen*, *Electronics*, *Clothing_Shoes_and_Jewelry*) kết hợp cùng *All_Beauty*.
- **Ưu điểm:** Giữ nguyên cấu trúc `user_id`, `parent_asin`, `timestamp`, `price`, `metadata` — tương thích trực tiếp với hệ thống FastAPI + SQLite của project `22120210`.

### Phương án B: eCommerce Behavior Data from Multi-Category Store (REES46 / Kaggle)
- **Quy mô:** ~285 triệu events trải dài 7 tháng (10/2019 – 04/2020), nén gzip ~15–20 GB, giải nén CSV khoảng **40–50 GB**.
- **Schema chuẩn streaming:** Đã có sẵn `event_time`, `event_type` (`view`, `cart`, `remove_from_cart`, `purchase`), `product_id`, `category_code`, `brand`, `price`, `user_id`, `user_session`.
- **Ưu điểm:** Phản ánh trực tiếp chuỗi hành vi mua sắm theo phiên (Session) để test Flink CEP và Abandoned Cart Timer.

---

## 2. Kịch bản vận hành: "Đêm Flash Sale 00:00"

Thay vì đọc file đều đặn theo kịch bản lý tưởng, hệ thống phải đối mặt với các biến cố đặc trưng của sàn TMĐT giờ cao điểm:

### A. Biến động tải & Lệch dữ liệu
1. **Flash Sale Spike (Giật tải 15x–20x):**
   - Tải nền ~1,500 events/s. Theo chu kỳ hoặc đột ngột, lưu lượng tăng vọt lên **25,000–35,000 events/s** trong 60 giây rồi giảm dần.
   - *Mục tiêu kiểm thử:* Quan sát Backpressure trên Flink UI, kích thước hàng đợi Redpanda, và thời gian hoàn tất Checkpoint khi tải cao.
2. **Hot Key Skew (Lệch phân phối Zipf cực đoan):**
   - 70% lượng truy cập trong giờ Flash Sale đổ dồn vào 1–2 sản phẩm khuyến mãi sâu.
   - *Vấn đề:* `keyBy(product_id)` thông thường sẽ dồn 70% tải lên duy nhất 1 Task Slot gây nghẽn cục bộ.
   - *Giải pháp Flink:* **Two-Phase Aggregation (Salting Key)** — gắn thêm hậu tố ngẫu nhiên `product_id + "_" + (0..9)` ở bước đếm cục bộ trước khi gộp tổng.

### B. Các Edge Cases bẩn trong Stream
1. **Out-of-Order & Late-Arriving Events (~5–10% traffic):**
   - Event `view` hoặc `cart` bị giữ lại từ 30 giây đến 4 phút rồi mới đẩy vào Redpanda *sau* event `purchase` (giả lập mạng di động mất kết nối tạm thời).
   - *Giải pháp Flink:* Dùng **Event Time + Watermark (`forBoundedOutOfOrderness`) + `allowedLateness`**. Dữ liệu quá hạn trễ hơn ngưỡng cho phép được tách qua **Side Output** để xử lý bù ở Cold Path.
2. **Duplicate Retry Storm (~3% traffic):**
   - App mobile không nhận được ACK lúc nghẽn mạng nên gửi lặp lại cùng một `event_id` 2–5 lần.
   - *Giải pháp Flink:* Deduplication bằng `ValueState` / `MapState` kết hợp `StateTtlConfig` (lưu `event_id` trong 15–30 phút).
3. **Bot săn sale & Zombie Session (~1% traffic):**
   - Một số `user_id` giả lập bot cào dữ liệu bắn liên tục 300–500 event `view`/giây.
   - *Hệ quả nếu xử lý sai:* `SessionWindow` không bao giờ đóng do gap luôn nhỏ hơn ngưỡng timeout, khiến `ListState` phình to vô hạn và gây OOM chết TaskManager.
   - *Giải pháp Flink:* Dùng **Flink CEP** hoặc `KeyedProcessFunction` phát hiện tần suất bất thường (> N events/5s) để đẩy vào blacklist Redis, đồng thời giới hạn kích thước lưu trữ lịch sử theo dạng Ring Buffer (tối đa 20 item gần nhất).
4. **Poison Pills & Schema Drift (~0.5% traffic):**
   - Bản ghi JSON bị cắt cụt, `price` âm hoặc chuyển từ `float` sang `string` (`"free"`), `user_id` null, hoặc timestamp sai lệch năm 1970 / 2099.
   - *Giải pháp Flink:* Bọc parser đẩy bản ghi lỗi sang **Dead Letter Queue (DLQ)** topic bằng Side Output thay vì để exception làm sập toàn bộ job.

---

## 3. Tác động trực tiếp lên FastAPI RecSys (`22120210`)

- **Real-time Negative Filtering:** Ngay khi event `purchase` đi qua Flink, sản phẩm đó được ghi vào tập loại trừ trên Redis trong < 1 giây để API `/recommend/{user_id}` không tiếp tục gợi ý món khách vừa mua xong.
- **Abandoned Cart Trigger (`OnTimer`):** Nếu user thêm hàng vào giỏ (`cart`) nhưng sau 2 phút Event Time vẫn chưa phát sinh `purchase`, `OnTimer` kích hoạt đẩy danh sách sản phẩm thay thế/combo đi kèm vào Redis để API hiển thị khi user quay lại.
