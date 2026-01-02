"""
Các hàm tiện ích cho project PySpark
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan, isnull, count, when

def print_dataframe_info(df: DataFrame, name: str = "DataFrame"):
    """In thông tin về DataFrame"""
    print(f"\n{'='*60}")
    print(f"Thông tin {name}")
    print(f"{'='*60}")
    print(f"Số dòng: {df.count():,}")
    print(f"Số cột: {len(df.columns)}")
    print(f"\nSchema:")
    df.printSchema()
    print(f"\n5 dòng đầu tiên:")
    df.show(5, truncate=False)

def count_missing_values(df: DataFrame):
    """Đếm số lượng giá trị missing trong mỗi cột"""
    missing_counts = df.select([
        count(when(isnull(c) | isnan(c), c)).alias(c) 
        for c in df.columns
    ])
    return missing_counts

def describe_dataframe(df: DataFrame):
    """Hiển thị thống kê mô tả của DataFrame"""
    print("\nThống kê mô tả:")
    df.describe().show(truncate=False)

def check_duplicates(df: DataFrame, columns=None):
    """Kiểm tra dòng trùng lặp"""
    if columns:
        total_rows = df.count()
        unique_rows = df.dropDuplicates(columns).count()
    else:
        total_rows = df.count()
        unique_rows = df.distinct().count()
    
    duplicates = total_rows - unique_rows
    print(f"\nTổng số dòng: {total_rows:,}")
    print(f"Số dòng unique: {unique_rows:,}")
    print(f"Số dòng trùng lặp: {duplicates:,}")
    
    return duplicates > 0

