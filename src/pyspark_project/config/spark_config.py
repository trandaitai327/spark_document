"""
Spark Session Configuration
"""
import os
from typing import Optional
from pyspark.sql import SparkSession
from pyspark import SparkConf


def create_spark_session(
    app_name: str = "PySpark Sales Analysis",
    master: Optional[str] = None,
    config_dict: Optional[dict] = None
) -> SparkSession:
    """
    Tạo SparkSession với cấu hình tối ưu
    
    Args:
        app_name: Tên ứng dụng Spark
        master: Spark master URL (mặc định: local[*])
        config_dict: Dictionary chứa các config bổ sung
        
    Returns:
        SparkSession object
    """
    # Cấu hình mặc định
    default_config = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.shuffle.partitions": "200",
        "spark.sql.autoBroadcastJoinThreshold": "50MB",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    }
    
    # Merge với config từ môi trường hoặc tham số
    if config_dict:
        default_config.update(config_dict)
    
    # Cấu hình từ environment variables
    env_config = {
        key.replace("SPARK_", "").lower().replace("_", "."): value
        for key, value in os.environ.items()
        if key.startswith("SPARK_")
    }
    default_config.update(env_config)
    
    conf = SparkConf().setAll(list(default_config.items()))
    
    builder = SparkSession.builder.appName(app_name)
    
    if master:
        builder = builder.master(master)
    else:
        builder = builder.master(os.getenv("SPARK_MASTER", "local[*]"))
    
    spark = builder.config(conf=conf).getOrCreate()
    
    # Set log level
    log_level = os.getenv("SPARK_LOG_LEVEL", "WARN")
    spark.sparkContext.setLogLevel(log_level)
    
    return spark

