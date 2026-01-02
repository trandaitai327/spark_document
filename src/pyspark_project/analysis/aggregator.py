"""
Aggregation Analysis Module
"""
from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, avg, count, collect_set


class Aggregator:
    """Class để thực hiện các phép aggregation"""
    
    @staticmethod
    def department_revenue(sales_enriched: DataFrame) -> DataFrame:
        """Tổng doanh thu theo phòng ban"""
        return sales_enriched.groupBy("employee_department").agg(
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_revenue"),
            count("*").alias("transaction_count")
        ).orderBy(col("total_revenue").desc())
    
    @staticmethod
    def top_products(sales_enriched: DataFrame, limit: int = 10) -> DataFrame:
        """Top sản phẩm bán chạy nhất"""
        return sales_enriched.groupBy("product_name", "category").agg(
            sum("quantity").alias("total_quantity_sold"),
            sum("total_amount").alias("total_revenue"),
            count("*").alias("transaction_count")
        ).orderBy(col("total_quantity_sold").desc()).limit(limit)
    
    @staticmethod
    def category_stats(sales_enriched: DataFrame) -> DataFrame:
        """Thống kê theo category"""
        from pyspark.sql.functions import min, max as max_func
        
        return sales_enriched.groupBy("category").agg(
            count("*").alias("transaction_count"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_revenue"),
            min("total_amount").alias("min_revenue"),
            max_func("total_amount").alias("max_revenue")
        ).orderBy(col("total_revenue").desc())
    
    @staticmethod
    def top_employees(sales_enriched: DataFrame, limit: int = 10) -> DataFrame:
        """Top nhân viên bán hàng giỏi nhất"""
        return sales_enriched.groupBy("employee_id", "employee_name", "employee_department").agg(
            sum("total_amount").alias("total_sales"),
            count("*").alias("transaction_count"),
            avg("total_amount").alias("avg_transaction_value")
        ).orderBy(col("total_sales").desc()).limit(limit)
    
    @staticmethod
    def employee_products(sales_enriched: DataFrame) -> DataFrame:
        """Danh sách sản phẩm mỗi nhân viên đã bán"""
        from pyspark.sql.functions import collect_list
        return sales_enriched.groupBy("employee_id", "employee_name").agg(
            collect_set("product_name").alias("products_sold"),
            count("product_name").alias("unique_products_count")
        )
    
    @staticmethod
    def quarterly_sales(sales_enriched: DataFrame) -> DataFrame:
        """Phân tích doanh thu theo quý"""
        return sales_enriched.groupBy(
            "sale_year",
            when(col("sale_month").between(1, 3), "Q1")
            .when(col("sale_month").between(4, 6), "Q2")
            .when(col("sale_month").between(7, 9), "Q3")
            .otherwise("Q4").alias("quarter")
        ).agg(
            sum("total_amount").alias("quarterly_revenue"),
            count("*").alias("transaction_count")
        ).orderBy("sale_year", "quarter")

