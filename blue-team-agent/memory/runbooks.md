# Blue Team SRE Runbooks

Sổ tay các lệnh kiểm tra nhanh và kinh nghiệm xử lý sự cố tích lũy cho hệ thống `flink-chaos-recsys`.

---

## 1. Kiểm tra nhanh trạng thái các tầng
- **Flink REST API:** `curl -s http://<PVE_FLINK_IP>:8081/jobs/overview`
- **Redpanda Lag:** `rpk group describe <consumer_group>`
- **Redis Health & Latency:** `redis-cli -h <PVE_DATA_IP> PING` và `redis-cli -h <PVE_DATA_IP> --latency`
- **FastAPI Health:** `curl -s http://localhost:8000/health`

---

## 2. Kinh nghiệm xử lý sự cố (Cập nhật dần sau các vòng GameDay)
*(Chưa có incident nào được ghi nhận)*
