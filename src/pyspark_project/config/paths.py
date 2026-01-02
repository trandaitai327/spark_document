"""
Path Configuration for Data and Output Directories
"""
import os
from pathlib import Path
from typing import Dict


def get_project_root() -> Path:
    """
    Lấy đường dẫn root của project
    
    Returns:
        Path object đến thư mục root
    """
    # Từ src/pyspark_project/config/paths.py
    # Lên 3 cấp để đến root: config -> pyspark_project -> src -> root
    current_file = Path(__file__).resolve()
    return current_file.parent.parent.parent.parent


def get_data_paths() -> Dict[str, str]:
    """
    Trả về đường dẫn đến các file dữ liệu input
    
    Returns:
        dict: Dictionary chứa các đường dẫn đến file dữ liệu
    """
    root = get_project_root()
    data_dir = root / "data" / "raw"
    
    return {
        "employees": str(data_dir / "employees.csv"),
        "products": str(data_dir / "products.csv"),
        "sales": str(data_dir / "sales.csv"),
        "departments": str(data_dir / "departments.csv"),
        "data_dir": str(data_dir),
    }


def get_output_paths() -> Dict[str, str]:
    """
    Trả về đường dẫn đến các thư mục output
    
    Returns:
        dict: Dictionary chứa các đường dẫn output
    """
    root = get_project_root()
    output_dir = root / "output"
    
    return {
        "base": str(output_dir),
        "parquet": str(output_dir / "parquet"),
        "csv": str(output_dir / "csv"),
        "reports": str(output_dir / "reports"),
    }


def ensure_directories():
    """Tạo các thư mục cần thiết nếu chưa tồn tại"""
    root = get_project_root()
    
    # Data directories
    (root / "data" / "raw").mkdir(parents=True, exist_ok=True)
    (root / "data" / "processed").mkdir(parents=True, exist_ok=True)
    
    # Output directories
    paths = get_output_paths()
    for path in paths.values():
        Path(path).mkdir(parents=True, exist_ok=True)
    
    # Logs directory
    (root / "logs").mkdir(parents=True, exist_ok=True)

