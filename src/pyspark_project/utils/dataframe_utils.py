"""
DataFrame utility functions
"""
from typing import Optional, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan, isnull, count, when
from pathlib import Path


def print_dataframe_info(df: DataFrame, name: str = "DataFrame", show_rows: int = 5):
    """
    In thông tin về DataFrame
    
    Args:
        df: DataFrame cần in thông tin
        name: Tên DataFrame để hiển thị
        show_rows: Số dòng để hiển thị
    """
    print(f"\n{'='*60}")
    print(f"Thông tin {name}")
    print(f"{'='*60}")
    print(f"Số dòng: {df.count():,}")
    print(f"Số cột: {len(df.columns)}")
    print(f"\nSchema:")
    df.printSchema()
    print(f"\n{show_rows} dòng đầu tiên:")
    df.show(show_rows, truncate=False)


def count_missing_values(df: DataFrame) -> DataFrame:
    """
    Đếm số lượng giá trị missing trong mỗi cột
    
    Args:
        df: DataFrame cần kiểm tra
        
    Returns:
        DataFrame chứa số lượng missing values cho mỗi cột
    """
    missing_counts = df.select([
        count(when(isnull(c) | isnan(c), c)).alias(c) 
        for c in df.columns
    ])
    return missing_counts


def describe_dataframe(df: DataFrame):
    """
    Hiển thị thống kê mô tả của DataFrame
    
    Args:
        df: DataFrame cần mô tả
    """
    print("\nThống kê mô tả:")
    df.describe().show(truncate=False)


def check_duplicates(df: DataFrame, columns: Optional[List[str]] = None) -> bool:
    """
    Kiểm tra dòng trùng lặp
    
    Args:
        df: DataFrame cần kiểm tra
        columns: Danh sách cột để kiểm tra duplicate (None = kiểm tra tất cả)
        
    Returns:
        True nếu có duplicate, False nếu không
    """
    total_rows = df.count()
    
    if columns:
        unique_rows = df.dropDuplicates(columns).count()
    else:
        unique_rows = df.distinct().count()
    
    duplicates = total_rows - unique_rows
    print(f"\nTổng số dòng: {total_rows:,}")
    print(f"Số dòng unique: {unique_rows:,}")
    print(f"Số dòng trùng lặp: {duplicates:,}")
    
    return duplicates > 0


def save_dataframe(
    df: DataFrame,
    path: str,
    format: str = "parquet",
    mode: str = "overwrite",
    coalesce: Optional[int] = None,
    **options
):
    """
    Lưu DataFrame với các tùy chọn
    
    Args:
        df: DataFrame cần lưu
        path: Đường dẫn đến file/thư mục
        format: Format file (parquet, csv, json, etc.)
        mode: Mode ghi (overwrite, append, ignore, error)
        coalesce: Số partition để coalesce trước khi ghi
        **options: Các tùy chọn bổ sung
    """
    # Đảm bảo thư mục tồn tại
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    
    # Coalesce nếu cần
    df_to_save = df.coalesce(coalesce) if coalesce else df
    
    # Ghi file
    writer = df_to_save.write.mode(mode)
    
    # Áp dụng các options
    for key, value in options.items():
        writer = writer.option(key, value)
    
    if format == "parquet":
        writer.parquet(path)
    elif format == "csv":
        if "header" not in options:
            writer.option("header", True)
        writer.csv(path)
    elif format == "json":
        writer.json(path)
    else:
        writer.format(format).save(path)
    
    print(f"✓ Đã lưu DataFrame -> {path} ({format})")

