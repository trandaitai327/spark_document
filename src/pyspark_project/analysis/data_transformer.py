"""
Data Transformation Module
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, datediff, current_date


class DataTransformer:
    """Class để transform dữ liệu"""
    
    @staticmethod
    def transform_employees(df: DataFrame) -> DataFrame:
        """Transform employees DataFrame"""
        return df.withColumn(
            "age_group",
            when(col("age") < 30, "Young")
            .when(col("age") < 50, "Middle")
            .otherwise("Senior")
        ).withColumn(
            "years_of_service",
            datediff(current_date(), col("hire_date")) / 365.0
        )
    
    @staticmethod
    def transform_products(df: DataFrame) -> DataFrame:
        """Transform products DataFrame"""
        return df.withColumn(
            "profit_margin",
            ((col("price") - col("cost")) / col("price") * 100).cast("double")
        )
    
    @staticmethod
    def transform_sales(df: DataFrame) -> DataFrame:
        """Transform sales DataFrame"""
        from pyspark.sql.functions import year, month, dayofmonth
        
        return df.withColumn("sale_year", year(col("sale_date"))) \
                 .withColumn("sale_month", month(col("sale_date"))) \
                 .withColumn("sale_day", dayofmonth(col("sale_date")))
    
    @staticmethod
    def enrich_sales(
        sales_df: DataFrame,
        employees_df: DataFrame,
        products_df: DataFrame
    ) -> DataFrame:
        """Join sales với employees và products"""
        return sales_df.join(
            employees_df,
            sales_df.employee_id == employees_df.employee_id,
            "inner"
        ).join(
            products_df,
            sales_df.product_id == products_df.product_id,
            "inner"
        ).select(
            sales_df["*"],
            employees_df["name"].alias("employee_name"),
            employees_df["department"].alias("employee_department"),
            products_df["product_name"],
            products_df["category"]
        )

