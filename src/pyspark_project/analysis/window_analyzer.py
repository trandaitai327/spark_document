"""
Window Functions Analysis Module
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, rank, row_number, lag, sum as sum_func
from pyspark.sql.window import Window


class WindowAnalyzer:
    """Class để thực hiện phân tích với Window Functions"""
    
    @staticmethod
    def rank_employees_by_department(employee_sales: DataFrame) -> DataFrame:
        """
        Ranking nhân viên theo doanh thu trong mỗi phòng ban
        
        Args:
            employee_sales: DataFrame với cột employee_id, employee_name, employee_department, total_sales
            
        Returns:
            DataFrame với cột rank và row_number
        """
        window_spec = Window.partitionBy("employee_department").orderBy(col("total_sales").desc())
        return employee_sales.withColumn("rank", rank().over(window_spec)) \
                            .withColumn("row_number", row_number().over(window_spec))
    
    @staticmethod
    def monthly_running_total(monthly_sales: DataFrame) -> DataFrame:
        """
        Running total doanh thu theo tháng
        
        Args:
            monthly_sales: DataFrame với cột sale_year, sale_month, monthly_revenue
            
        Returns:
            DataFrame với cột running_total
        """
        window_spec = Window.orderBy("sale_year", "sale_month").rowsBetween(
            Window.unboundedPreceding, Window.currentRow
        )
        return monthly_sales.withColumn(
            "running_total", sum_func("monthly_revenue").over(window_spec)
        )
    
    @staticmethod
    def monthly_comparison(monthly_sales: DataFrame) -> DataFrame:
        """
        So sánh doanh thu với tháng trước
        
        Args:
            monthly_sales: DataFrame với cột sale_year, sale_month, monthly_revenue
            
        Returns:
            DataFrame với cột prev_month_revenue, revenue_change, percent_change
        """
        window_spec = Window.orderBy("sale_year", "sale_month")
        return monthly_sales.withColumn(
            "prev_month_revenue",
            lag("monthly_revenue", 1).over(window_spec)
        ).withColumn(
            "revenue_change",
            col("monthly_revenue") - col("prev_month_revenue")
        ).withColumn(
            "percent_change",
            ((col("monthly_revenue") - col("prev_month_revenue")) / col("prev_month_revenue") * 100).cast("double")
        )

