# PySpark Project - Sales Data Analysis

Project PySpark hoàn chỉnh để phân tích dữ liệu bán hàng với các tính năng từ cơ bản đến nâng cao.

## Cấu Trúc Project

```
.
├── src/
│   ├── __init__.py
│   ├── config.py          # Cấu hình Spark
│   ├── generate_data.py   # Tạo dữ liệu mẫu
│   ├── main.py            # Script chính
│   └── utils.py           # Các hàm tiện ích
├── data/
│   ├── raw/               # Dữ liệu thô
│   └── processed/         # Dữ liệu đã xử lý
├── output/                # Kết quả output
├── requirements.txt
├── README.md
└── .gitignore
```

## Cài Đặt

### 1. Cài đặt Python dependencies

```bash
pip install -r requirements.txt
```

### 2. Yêu cầu hệ thống

- Python 3.8+
- Java 8 hoặc cao hơn (bắt buộc cho PySpark)
- Tối thiểu 4GB RAM

### 3. Kiểm tra Java

```bash
java -version
```

## Sử Dụng

### 1. Tạo dữ liệu mẫu

```bash
python src/generate_data.py
```

Lệnh này sẽ tạo các file CSV mẫu trong thư mục `data/raw/`:
- `employees.csv` - Thông tin nhân viên
- `products.csv` - Thông tin sản phẩm
- `sales.csv` - Dữ liệu bán hàng
- `departments.csv` - Thông tin phòng ban

### 2. Chạy phân tích

```bash
python src/main.py
```

Script này sẽ:
- Đọc dữ liệu từ CSV
- Thực hiện các phép transform và aggregation
- Sử dụng Window Functions
- Tạo các báo cáo thống kê
- Ghi kết quả ra file Parquet và CSV

## Tính Năng

Project này demo các tính năng PySpark:

### Cơ Bản
- ✅ Đọc/Ghi CSV, Parquet
- ✅ DataFrame operations (filter, select, groupBy)
- ✅ Aggregation functions
- ✅ Join operations
- ✅ Missing values handling

### Nâng Cao
- ✅ Window Functions
- ✅ User Defined Functions (UDF)
- ✅ Broadcast variables
- ✅ Partitioning strategies
- ✅ Caching và persistence
- ✅ Performance optimization

## Output

Kết quả được lưu trong thư mục `output/`:
- `employee_stats.parquet` - Thống kê nhân viên
- `sales_summary.parquet` - Tóm tắt bán hàng
- `top_products.csv` - Top sản phẩm bán chạy
- `department_performance.csv` - Hiệu suất phòng ban

## Tùy Chỉnh

Bạn có thể chỉnh sửa:
- `src/config.py` - Cấu hình Spark
- `src/generate_data.py` - Thay đổi số lượng dữ liệu mẫu
- `src/main.py` - Thêm các phép phân tích mới

## Ghi Chú

- Dữ liệu mẫu được tạo ngẫu nhiên
- Project này dùng cho mục đích học tập và demo
- Tùy chỉnh cấu hình Spark trong `src/config.py` dựa trên tài nguyên máy của bạn

