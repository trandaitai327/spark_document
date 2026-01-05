# PySpark Sales Data Analysis Project

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5+-orange.svg)](https://spark.apache.org/docs/latest/api/python/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Một project PySpark hoàn chỉnh và chuyên nghiệp để phân tích dữ liệu bán hàng với các tính năng từ cơ bản đến nâng cao.

## 📋 Mục Lục

- [Tính Năng](#tính-năng)
- [Cấu Trúc Project](#cấu-trúc-project)
- [Yêu Cầu Hệ Thống](#yêu-cầu-hệ-thống)
- [Cài Đặt](#cài-đặt)
- [Sử Dụng](#sử-dụng)
- [Cấu Trúc Code](#cấu-trúc-code)
- [Tài Liệu](#tài-liệu)
- [Đóng Góp](#đóng-góp)
- [License](#license)

## ✨ Tính Năng

### Cơ Bản
- ✅ Đọc/Ghi dữ liệu CSV, Parquet
- ✅ DataFrame operations (filter, select, groupBy)
- ✅ Aggregation functions (sum, avg, count, min, max)
- ✅ Join operations (inner, left, right, outer)
- ✅ Xử lý missing values

### Nâng Cao
- ✅ Window Functions (ranking, running totals, LAG/LEAD)
- ✅ User Defined Functions (UDF)
- ✅ Broadcast variables và Accumulators
- ✅ Partitioning strategies
- ✅ Caching và persistence
- ✅ Performance optimization với Adaptive Query Execution (AQE)

### Phân Tích Dữ Liệu
- ✅ Phân tích doanh thu theo phòng ban
- ✅ Top sản phẩm và nhân viên
- ✅ Thống kê theo category
- ✅ Phân tích theo thời gian (tháng, quý)
- ✅ Running totals và so sánh tháng trước

## 📁 Cấu Trúc Project

```
.
├── src/
│   └── pyspark_project/          # Package chính
│       ├── __init__.py
│       ├── config/               # Cấu hình
│       │   ├── __init__.py
│       │   ├── spark_config.py   # Cấu hình Spark
│       │   └── paths.py          # Quản lý đường dẫn
│       ├── data/                 # Data generation
│       │   ├── __init__.py
│       │   └── generator.py      # Tạo dữ liệu mẫu
│       ├── analysis/             # Phân tích
│       │   ├── __init__.py
│       │   ├── data_loader.py    # Load dữ liệu
│       │   ├── data_transformer.py  # Transform
│       │   ├── aggregator.py     # Aggregation
│       │   └── window_analyzer.py  # Window functions
│       └── utils/                # Utilities
│           ├── __init__.py
│           └── dataframe_utils.py
├── scripts/                      # Scripts chạy
│   ├── generate_data.py
│   └── run_analysis.py
├── data/                         # Dữ liệu
│   ├── raw/                      # Dữ liệu thô
│   └── processed/                # Dữ liệu đã xử lý
├── output/                       # Kết quả
│   ├── parquet/                  # Output Parquet
│   ├── csv/                      # Output CSV
│   └── reports/                  # Báo cáo
├── docs/                         # Tài liệu
│   ├── pyspark-co-ban.md
│   ├── pyspark-nang-cao.md
│   └── *.pdf
├── tests/                        # Unit tests
├── config/                       # File cấu hình
├── setup.py                      # Setup script
├── pyproject.toml               # Project metadata
├── requirements.txt             # Dependencies
├── README.md                    # File này
└── LICENSE                      # License
```

## 🔧 Yêu Cầu Hệ Thống

- **Python**: 3.8 hoặc cao hơn
- **Java**: 8 hoặc cao hơn (bắt buộc cho PySpark)
- **RAM**: Tối thiểu 4GB (khuyến nghị 8GB+)
- **Disk**: ~500MB cho cài đặt

## 📦 Cài Đặt

### 1. Clone repository

```bash
git clone https://github.com/trandaitai327/spark_document.git
cd Spark
```

### 2. Tạo virtual environment (khuyến nghị)

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

### 3. Cài đặt dependencies

```bash
pip install -r requirements.txt
```

Hoặc cài đặt như một package:

```bash
pip install -e .
```

### 4. Kiểm tra cài đặt

```bash
python -c "import pyspark; print(pyspark.__version__)"
java -version
```

## 🚀 Sử Dụng

### 1. Tạo dữ liệu mẫu

```bash
python scripts/generate_data.py
```

Hoặc:

```bash
cd scripts
python generate_data.py
```

Lệnh này sẽ tạo các file CSV mẫu trong thư mục `data/raw/`:
- `employees.csv` - 100 nhân viên
- `products.csv` - 50 sản phẩm
- `sales.csv` - 10,000 giao dịch bán hàng
- `departments.csv` - 6 phòng ban

### 2. Chạy phân tích

```bash
python scripts/run_analysis.py
```

Hoặc:

```bash
cd scripts
python run_analysis.py
```

Script này sẽ thực hiện:
1. Đọc dữ liệu từ CSV
2. Transform và enrich dữ liệu
3. Thực hiện các phép aggregation
4. Sử dụng Window Functions
5. Lưu kết quả ra Parquet và CSV

### 3. Xem kết quả

Kết quả được lưu trong thư mục `output/`:
- `parquet/` - Các file Parquet
- `csv/` - Các file CSV (top products, employees, departments)
- `reports/` - Báo cáo (nếu có)

## 📚 Cấu Trúc Code

### Config Module

```python
from pyspark_project.config import create_spark_session, get_data_paths

spark = create_spark_session("My App")
paths = get_data_paths()
```

### Data Generation

```python
from pyspark_project.data import DataGenerator

generator = DataGenerator()
generator.generate_all(n_employees=100, n_products=50, n_sales=10000)
```

### Analysis

```python
from pyspark_project.analysis import DataLoader, DataTransformer, Aggregator

loader = DataLoader(spark)
employees, products, sales, departments = loader.load_all()

transformer = DataTransformer()
sales_enriched = transformer.enrich_sales(sales, employees, products)

aggregator = Aggregator()
top_products = aggregator.top_products(sales_enriched, limit=10)
```

## 📖 Tài Liệu

Tài liệu chi tiết về PySpark được lưu trong thư mục `docs/`:

- `pyspark-co-ban.md` - Hướng dẫn PySpark cơ bản
- `pyspark-nang-cao.md` - Hướng dẫn PySpark nâng cao
- Các file PDF tương ứng

## ⚙️ Cấu Hình

### Environment Variables

Bạn có thể cấu hình Spark thông qua environment variables:

```bash
export SPARK_MASTER="local[*]"
export SPARK_LOG_LEVEL="WARN"
export SPARK_SQL_SHUFFLE_PARTITIONS="200"
```

### Spark Config

Chỉnh sửa `src/pyspark_project/config/spark_config.py` để tùy chỉnh cấu hình Spark.

## 🧪 Testing

```bash
pytest tests/
```

## 🤝 Đóng Góp

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the project
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👤 Author

Đại Tài - trandaitai327@gmail.com

## 🙏 Acknowledgments

- Apache Spark community
- PySpark documentation
- All contributors

---

**Note**: Project này dùng cho mục đích học tập và demo. Điều chỉnh cấu hình và code theo nhu cầu thực tế của bạn.
