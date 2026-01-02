"""
Script chính để chạy phân tích
"""
import sys
from pathlib import Path

# Thêm src vào path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from pyspark_project.config.spark_config import create_spark_session
from pyspark_project.config.paths import get_output_paths, ensure_directories
from pyspark_project.analysis.data_loader import DataLoader
from pyspark_project.analysis.data_transformer import DataTransformer
from pyspark_project.analysis.aggregator import Aggregator
from pyspark_project.analysis.window_analyzer import WindowAnalyzer
from pyspark_project.utils.dataframe_utils import save_dataframe
from pyspark.sql.functions import sum as sum_func, year, month, col


def main():
    """Hàm chính"""
    print("\n" + "="*60)
    print("PYSPARK SALES DATA ANALYSIS")
    print("="*60)
    
    # Đảm bảo các thư mục tồn tại
    ensure_directories()
    
    # Tạo SparkSession
    spark = create_spark_session("Sales Data Analysis")
    output_paths = get_output_paths()
    
    try:
        # 1. Load dữ liệu
        loader = DataLoader(spark)
        employees_df, products_df, sales_df, departments_df = loader.load_all(show_info=True)
        
        # 2. Transform dữ liệu
        print("\n" + "="*60)
        print("BƯỚC 2: TRANSFORM DỮ LIỆU")
        print("="*60)
        
        transformer = DataTransformer()
        employees_transformed = transformer.transform_employees(employees_df)
        products_transformed = transformer.transform_products(products_df)
        sales_transformed = transformer.transform_sales(sales_df)
        
        print("\n2.1. Transform Employees - Thêm cột age_group và years_of_service")
        employees_transformed.select("employee_id", "name", "age", "age_group", "years_of_service").show(10)
        
        print("\n2.2. Transform Products - Tính profit margin")
        products_transformed.select("product_id", "product_name", "price", "cost", "profit_margin").show(10)
        
        print("\n2.3. Transform Sales - Thêm cột thời gian")
        sales_transformed.select("sale_id", "sale_date", "sale_year", "sale_month", "total_amount").show(10)
        
        # 3. Join dữ liệu
        print("\n" + "="*60)
        print("BƯỚC 3: JOIN DỮ LIỆU")
        print("="*60)
        
        sales_enriched = transformer.enrich_sales(sales_transformed, employees_transformed, products_transformed)
        sales_enriched.cache()
        print("\n3.1. Join Sales với Employees và Products")
        sales_enriched.show(10, truncate=False)
        
        # 4. Aggregation
        print("\n" + "="*60)
        print("BƯỚC 4: AGGREGATION ANALYSIS")
        print("="*60)
        
        aggregator = Aggregator()
        
        dept_revenue = aggregator.department_revenue(sales_enriched)
        print("\n4.1. Tổng doanh thu theo phòng ban")
        dept_revenue.show(truncate=False)
        
        top_products = aggregator.top_products(sales_enriched, limit=10)
        print("\n4.2. Top 10 sản phẩm bán chạy nhất")
        top_products.show(truncate=False)
        
        category_stats = aggregator.category_stats(sales_enriched)
        print("\n4.3. Thống kê theo category")
        category_stats.show(truncate=False)
        
        top_employees = aggregator.top_employees(sales_enriched, limit=10)
        print("\n4.4. Top 10 nhân viên bán hàng giỏi nhất")
        top_employees.show(truncate=False)
        
        # 5. Window Functions
        print("\n" + "="*60)
        print("BƯỚC 5: WINDOW FUNCTIONS ANALYSIS")
        print("="*60)
        
        window_analyzer = WindowAnalyzer()
        
        # Tạo employee_sales cho window functions
        from pyspark.sql.functions import sum as sum_func
        employee_sales = sales_enriched.groupBy("employee_id", "employee_name", "employee_department").agg(
            sum_func("total_amount").alias("total_sales")
        )
        
        employee_ranked = window_analyzer.rank_employees_by_department(employee_sales)
        print("\n5.1. Ranking nhân viên theo doanh thu trong mỗi phòng ban (Top 5 mỗi phòng)")
        employee_ranked.filter(col("rank") <= 5).show(truncate=False)
        
        monthly_sales = sales_enriched.groupBy("sale_year", "sale_month").agg(
            sum_func("total_amount").alias("monthly_revenue")
        ).orderBy("sale_year", "sale_month")
        
        monthly_running = window_analyzer.monthly_running_total(monthly_sales)
        print("\n5.2. Running total doanh thu theo tháng")
        monthly_running.show(truncate=False)
        
        monthly_with_lag = window_analyzer.monthly_comparison(monthly_sales)
        print("\n5.3. So sánh doanh thu với tháng trước")
        monthly_with_lag.show(truncate=False)
        
        # 6. Advanced Analysis
        print("\n" + "="*60)
        print("BƯỚC 6: ADVANCED ANALYSIS")
        print("="*60)
        
        employee_products = aggregator.employee_products(sales_enriched)
        print("\n6.1. Danh sách sản phẩm mỗi nhân viên đã bán")
        employee_products.show(5, truncate=False)
        
        quarterly_sales = aggregator.quarterly_sales(sales_enriched)
        print("\n6.2. Phân tích doanh thu theo quý")
        quarterly_sales.show(truncate=False)
        
        # 7. Lưu kết quả
        print("\n" + "="*60)
        print("BƯỚC 7: LƯU KẾT QUẢ")
        print("="*60)
        
        results = [
            (dept_revenue, "department_revenue"),
            (top_products, "top_products"),
            (category_stats, "category_stats"),
            (top_employees, "top_employees"),
            (employee_ranked, "employee_ranked"),
            (monthly_running, "monthly_running_total"),
            (quarterly_sales, "quarterly_sales")
        ]
        
        for df, name in results:
            # Lưu Parquet
            parquet_path = f"{output_paths['parquet']}/{name}.parquet"
            save_dataframe(df, parquet_path, format="parquet")
            
            # Lưu CSV cho các bảng quan trọng
            if "top" in name.lower() or "summary" in name.lower() or "department" in name.lower():
                csv_path = f"{output_paths['csv']}/{name}.csv"
                save_dataframe(df, csv_path, format="csv", coalesce=1)
        
        sales_enriched.unpersist()
        
        print("\n" + "="*60)
        print("✓ HOÀN THÀNH PHÂN TÍCH!")
        print("="*60)
        print(f"\nKết quả đã được lưu trong thư mục: {output_paths['base']}/")
        
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()
        print("\nSparkSession đã được đóng.")


if __name__ == "__main__":
    main()

