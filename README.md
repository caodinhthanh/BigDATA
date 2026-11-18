# Steam Reviews Big Data

Pipeline batch cho đồ án, chạy trên một máy Windows có Docker Desktop và WSL 2. Kiến trúc hai DAG tham khảo [Steam Games Data Pipeline](https://github.com/Khalid-Sobh/steam-games-data-pipeline).

| Bước | Xử lý |
| --- | --- |
| DAG 1 | CSV → MinIO → PySpark local → Parquet chi tiết theo tháng và Parquet tổng hợp trên MinIO |
| DAG 2 | Parquet tổng hợp → PostgreSQL → dbt build và data tests |
| Báo cáo | Power BI Desktop đọc ba view PostgreSQL do dbt tạo |

Spark chạy ở chế độ local trong container scheduler. Cấu hình mặc định là hai luồng, 2 GiB heap và 24 shuffle partitions. Có thể đổi trong `.env`. PostgreSQL dùng hai database riêng cho metadata Airflow và kho phân tích.

## Chạy trên Windows

Mở Docker Desktop và xác nhận `docker version` trả về cả Client lẫn Server. Trong PowerShell tại thư mục project, chạy:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run-local.ps1
```

Script build image, bật dịch vụ, kích hoạt DAG đầu, đợi hai DAG chạy thành công và kiểm tra tổng số review. Dữ liệu mẫu có **6 review**, trong đó **4 đề xuất**.

| Dịch vụ | Địa chỉ mặc định | Đăng nhập mặc định |
| --- | --- | --- |
| Airflow | http://localhost:8080 | `admin` / `steam_local` |
| MinIO Console | http://localhost:9001 | `steamadmin` / `steam_local_minio` |
| PostgreSQL | `localhost:5433`, database `steam_reviews` | `steam` / `steam_local` |

Nếu cổng bị trùng, sao chép `.env.example` thành `.env` rồi đổi cổng tương ứng. Dùng `docker compose ps` để xem trạng thái và `docker compose logs airflow-scheduler` để chẩn đoán khởi động. Xem log từng task trong Airflow. Dùng `docker compose down` để dừng dịch vụ và giữ volume.

## Chạy toàn bộ dữ liệu Kaggle

1. Tải [Steam Reviews](https://www.kaggle.com/datasets/forgemaster/steam-reviews-dataset) và giải nén 11 CSV vào `data/raw`, hoặc đặt `STEAM_DATA_DIR` trong `.env` để trỏ đến thư mục CSV có sẵn.
2. Docker Desktop quản lý image và volume theo cấu hình trên máy. Có thể đặt `MINIO_DATA_DIR` trong `.env` để chọn nơi lưu dữ liệu MinIO. Nếu image đã được dựng, có thể thêm `-SkipBuild` khi chạy lại.
3. Chạy lệnh sau:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run-local.ps1 -Source raw -TimeoutMinutes 120
```

DAG chạy theo yêu cầu. CSV có cùng nội dung được tái sử dụng trên MinIO. Mỗi lần chạy có thư mục Parquet riêng. PostgreSQL thay dữ liệu tổng hợp trong một transaction. Dữ liệu mẫu và dữ liệu Kaggle dùng các schema riêng.

Spark giữ các review có mã game, tháng và giá trị đề xuất hợp lệ. Thời gian chơi hoặc số review của tác giả bị thiếu được nhóm vào `unknown`. Cấu hình mặc định đọc thời gian chơi theo phút. Đặt `PLAYTIME_UNIT=hours` nếu dữ liệu đầu vào đã đổi sang giờ. Chỉ số hoạt động dựa trên số review của tác giả trong dữ liệu nguồn, tính theo từng review.

## Power BI Desktop

Mở `dashboard/SteamReviews.pbip` và bấm **Refresh** để nạp dữ liệu từ PostgreSQL. Nếu Power BI hỏi thông tin đăng nhập, chọn **Database** và dùng tài khoản PostgreSQL trong bảng trên. Để tạo báo cáo mới, chọn **Get data → PostgreSQL database**, nhập server `localhost:5433` và database `steam_reviews`. Chọn schema `analytics_demo` khi chạy thử hoặc `analytics` khi chạy dữ liệu Kaggle.

| View | Biểu đồ |
| --- | --- |
| `recommendation_trend` | Tỷ lệ đề xuất theo tháng và game |
| `playtime_behavior` | Tỷ lệ đề xuất theo nhóm thời gian chơi |
| `reviewer_activity` | Tỷ lệ đề xuất theo nhóm hoạt động của người đánh giá |

Tạo measure trên từng bảng dùng để vẽ biểu đồ. Khi đổi bộ lọc, lấy tổng tử số chia tổng mẫu số:

```dax
Recommendation % = DIVIDE(SUM(recommendation_trend[recommendations]), SUM(recommendation_trend[reviews]))
Average playtime hours = DIVIDE(SUM(recommendation_trend[total_playtime_minutes]), 60 * SUM(recommendation_trend[known_playtime]))
```

Bấm **Refresh** trong Power BI Desktop sau khi Airflow chạy xong nếu dùng chế độ Import.
