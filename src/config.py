"""
Cấu hình Spark Session
"""
from pyspark.sql import SparkSession
from pyspark import SparkConf

def create_spark_session(app_name: str = "PySpark Sales Analysis"):
    """
    Tạo SparkSession với cấu hình tối ưu
    
    Args:
        app_name: Tên ứng dụng Spark
        
    Returns:
        SparkSession object
    """
    conf = SparkConf().setAll([
        ("spark.sql.adaptive.enabled", "true"),
        ("spark.sql.adaptive.coalescePartitions.enabled", "true"),
        ("spark.sql.shuffle.partitions", "200"),
        ("spark.sql.autoBroadcastJoinThreshold", "50MB"),
        ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
    ])
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config(conf=conf) \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def get_data_paths():
    """
    Trả về đường dẫn đến các file dữ liệu
    
    Returns:
        dict: Dictionary chứa các đường dẫn
    """
    return {
        "employees": "data/raw/employees.csv",
        "products": "data/raw/products.csv",
        "sales": "data/raw/sales.csv",
        "departments": "data/raw/departments.csv",
        "output": "output"
    }

