# Project Structure Documentation

## Cấu Trúc Thư Mục

```
Spark/
├── src/                           # Source code
│   └── pyspark_project/          # Main package
│       ├── __init__.py           # Package initialization
│       ├── config/               # Configuration module
│       │   ├── __init__.py
│       │   ├── spark_config.py   # Spark session configuration
│       │   └── paths.py          # Path management
│       ├── data/                 # Data generation module
│       │   ├── __init__.py
│       │   └── generator.py      # Sample data generator
│       ├── analysis/             # Analysis module
│       │   ├── __init__.py
│       │   ├── data_loader.py    # Data loading
│       │   ├── data_transformer.py  # Data transformation
│       │   ├── aggregator.py     # Aggregation operations
│       │   └── window_analyzer.py  # Window functions
│       └── utils/                # Utilities module
│           ├── __init__.py
│           └── dataframe_utils.py  # DataFrame utilities
│
├── scripts/                      # Executable scripts
│   ├── __init__.py
│   ├── generate_data.py         # Generate sample data
│   └── run_analysis.py          # Run analysis pipeline
│
├── data/                         # Data directories
│   ├── raw/                      # Raw input data (CSV files)
│   └── processed/                # Processed data
│
├── output/                       # Output results
│   ├── parquet/                  # Parquet format outputs
│   ├── csv/                      # CSV format outputs
│   └── reports/                  # Report files
│
├── docs/                         # Documentation
│   ├── pyspark-co-ban.md        # Basic PySpark guide
│   ├── pyspark-co-ban.pdf
│   ├── pyspark-nang-cao.md      # Advanced PySpark guide
│   └── pyspark-nang-cao.pdf
│
├── tests/                        # Unit tests
│   └── (test files)
│
├── config/                       # Configuration files
│   └── (config files)
│
├── setup.py                      # Setup script
├── pyproject.toml               # Project metadata (PEP 518)
├── requirements.txt             # Python dependencies
├── README.md                    # Main documentation
├── LICENSE                      # License file
└── .gitignore                   # Git ignore rules
```

## Module Descriptions

### config/
- **spark_config.py**: Cấu hình SparkSession với các settings tối ưu
- **paths.py**: Quản lý tất cả các đường dẫn trong project

### data/
- **generator.py**: Class DataGenerator để tạo dữ liệu mẫu

### analysis/
- **data_loader.py**: Class DataLoader để load dữ liệu từ các nguồn
- **data_transformer.py**: Class DataTransformer để transform dữ liệu
- **aggregator.py**: Class Aggregator chứa các phép aggregation
- **window_analyzer.py**: Class WindowAnalyzer cho window functions

### utils/
- **dataframe_utils.py**: Các hàm tiện ích cho DataFrame operations

## Design Principles

1. **Separation of Concerns**: Mỗi module có trách nhiệm rõ ràng
2. **Reusability**: Code được tổ chức để dễ tái sử dụng
3. **Maintainability**: Cấu trúc rõ ràng, dễ bảo trì
4. **Scalability**: Dễ mở rộng với các tính năng mới
5. **Professional Standards**: Tuân thủ best practices của Python projects

## Import Patterns

```python
# Config
from pyspark_project.config import create_spark_session, get_data_paths

# Data
from pyspark_project.data import DataGenerator

# Analysis
from pyspark_project.analysis import (
    DataLoader,
    DataTransformer,
    Aggregator,
    WindowAnalyzer
)

# Utils
from pyspark_project.utils import (
    print_dataframe_info,
    save_dataframe
)
```

