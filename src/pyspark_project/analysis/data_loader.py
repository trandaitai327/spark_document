"""
Data Loading Module
"""
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame

from ..config.paths import get_data_paths
from ..utils.dataframe_utils import print_dataframe_info


class DataLoader:
    """Class để load dữ liệu từ các nguồn"""
    
    def __init__(self, spark: SparkSession):
        """
        Khởi tạo DataLoader
        
        Args:
            spark: SparkSession object
        """
        self.spark = spark
        self.paths = get_data_paths()
    
    def load_all(self, show_info: bool = True) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
        """
        Load tất cả dữ liệu
        
        Args:
            show_info: Có hiển thị thông tin DataFrame không
            
        Returns:
            Tuple chứa (employees_df, products_df, sales_df, departments_df)
        """
        print("\n" + "="*60)
        print("BƯỚC 1: ĐỌC DỮ LIỆU")
        print("="*60)
        
        # Đọc employees
        print("\nĐang đọc employees...")
        employees_df = self.spark.read.option("header", True).option("inferSchema", True).csv(
            self.paths["employees"]
        )
        if show_info:
            print_dataframe_info(employees_df, "Employees")
        
        # Đọc products
        print("\nĐang đọc products...")
        products_df = self.spark.read.option("header", True).option("inferSchema", True).csv(
            self.paths["products"]
        )
        if show_info:
            print_dataframe_info(products_df, "Products")
        
        # Đọc sales
        print("\nĐang đọc sales...")
        sales_df = self.spark.read.option("header", True).option("inferSchema", True).csv(
            self.paths["sales"]
        )
        if show_info:
            print_dataframe_info(sales_df, "Sales")
        
        # Đọc departments
        print("\nĐang đọc departments...")
        departments_df = self.spark.read.option("header", True).option("inferSchema", True).csv(
            self.paths["departments"]
        )
        if show_info:
            print_dataframe_info(departments_df, "Departments")
        
        return employees_df, products_df, sales_df, departments_df

