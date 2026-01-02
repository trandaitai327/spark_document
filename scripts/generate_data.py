"""
Script để tạo dữ liệu mẫu
"""
import sys
from pathlib import Path

# Thêm src vào path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from pyspark_project.data.generator import DataGenerator
from pyspark_project.config.paths import ensure_directories


def main():
    """Hàm chính"""
    # Đảm bảo các thư mục tồn tại
    ensure_directories()
    
    # Tạo generator và generate dữ liệu
    generator = DataGenerator()
    generator.generate_all(
        n_employees=100,
        n_products=50,
        n_sales=10000
    )


if __name__ == "__main__":
    main()

