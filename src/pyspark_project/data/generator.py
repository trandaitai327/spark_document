"""
Data Generator Module
Tạo dữ liệu mẫu cho project
"""
import pandas as pd
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from ..config.paths import get_project_root, get_data_paths


class DataGenerator:
    """Class để tạo dữ liệu mẫu"""
    
    def __init__(self, output_dir: Optional[str] = None):
        """
        Khởi tạo DataGenerator
        
        Args:
            output_dir: Thư mục output (mặc định: data/raw từ config)
        """
        if output_dir:
            self.output_dir = Path(output_dir)
        else:
            paths = get_data_paths()
            self.output_dir = Path(paths["data_dir"])
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_employees(self, n: int = 100) -> pd.DataFrame:
        """Tạo dữ liệu nhân viên"""
        departments = ["Sales", "IT", "Marketing", "HR", "Finance", "Operations"]
        positions = ["Manager", "Senior", "Junior", "Intern", "Director"]
        
        data = []
        for i in range(1, n + 1):
            data.append({
                "employee_id": i,
                "name": f"Employee_{i}",
                "department": random.choice(departments),
                "position": random.choice(positions),
                "salary": random.randint(3000, 15000),
                "hire_date": (datetime.now() - timedelta(days=random.randint(0, 3650))).strftime("%Y-%m-%d"),
                "age": random.randint(22, 65)
            })
        
        return pd.DataFrame(data)
    
    def generate_products(self, n: int = 50) -> pd.DataFrame:
        """Tạo dữ liệu sản phẩm"""
        categories = ["Electronics", "Clothing", "Food", "Books", "Toys", "Home"]
        
        data = []
        for i in range(1, n + 1):
            base_price = random.randint(10, 1000)
            data.append({
                "product_id": i,
                "product_name": f"Product_{i}",
                "category": random.choice(categories),
                "price": base_price,
                "cost": int(base_price * random.uniform(0.4, 0.7)),
                "stock_quantity": random.randint(0, 500)
            })
        
        return pd.DataFrame(data)
    
    def generate_sales(self, n: int = 10000, employee_count: int = 100, product_count: int = 50) -> pd.DataFrame:
        """Tạo dữ liệu bán hàng"""
        data = []
        start_date = datetime.now() - timedelta(days=365)
        
        for i in range(1, n + 1):
            sale_date = start_date + timedelta(days=random.randint(0, 365))
            quantity = random.randint(1, 10)
            product_id = random.randint(1, product_count)
            employee_id = random.randint(1, employee_count)
            price = random.randint(20, 500)
            
            data.append({
                "sale_id": i,
                "product_id": product_id,
                "employee_id": employee_id,
                "sale_date": sale_date.strftime("%Y-%m-%d"),
                "quantity": quantity,
                "unit_price": price,
                "total_amount": quantity * price
            })
        
        return pd.DataFrame(data)
    
    def generate_departments(self) -> pd.DataFrame:
        """Tạo dữ liệu phòng ban"""
        data = [
            {"department_id": 1, "department_name": "Sales", "location": "Hanoi", "budget": 500000},
            {"department_id": 2, "department_name": "IT", "location": "Ho Chi Minh", "budget": 800000},
            {"department_id": 3, "department_name": "Marketing", "location": "Hanoi", "budget": 400000},
            {"department_id": 4, "department_name": "HR", "location": "Da Nang", "budget": 300000},
            {"department_id": 5, "department_name": "Finance", "location": "Hanoi", "budget": 600000},
            {"department_id": 6, "department_name": "Operations", "location": "Ho Chi Minh", "budget": 700000},
        ]
        return pd.DataFrame(data)
    
    def generate_all(
        self,
        n_employees: int = 100,
        n_products: int = 50,
        n_sales: int = 10000
    ) -> dict:
        """
        Tạo tất cả dữ liệu
        
        Args:
            n_employees: Số lượng nhân viên
            n_products: Số lượng sản phẩm
            n_sales: Số lượng giao dịch bán hàng
            
        Returns:
            Dictionary chứa các DataFrame đã tạo
        """
        print("Bắt đầu tạo dữ liệu mẫu...")
        
        # Tạo dữ liệu
        print(f"\nTạo {n_employees} employees...")
        employees_df = self.generate_employees(n_employees)
        employees_path = self.output_dir / "employees.csv"
        employees_df.to_csv(employees_path, index=False)
        print(f"✓ Đã tạo {len(employees_df)} nhân viên -> {employees_path}")
        
        print(f"\nTạo {n_products} products...")
        products_df = self.generate_products(n_products)
        products_path = self.output_dir / "products.csv"
        products_df.to_csv(products_path, index=False)
        print(f"✓ Đã tạo {len(products_df)} sản phẩm -> {products_path}")
        
        print(f"\nTạo {n_sales} sales transactions...")
        sales_df = self.generate_sales(n_sales, n_employees, n_products)
        sales_path = self.output_dir / "sales.csv"
        sales_df.to_csv(sales_path, index=False)
        print(f"✓ Đã tạo {len(sales_df)} giao dịch bán hàng -> {sales_path}")
        
        print("\nTạo departments...")
        departments_df = self.generate_departments()
        departments_path = self.output_dir / "departments.csv"
        departments_df.to_csv(departments_path, index=False)
        print(f"✓ Đã tạo {len(departments_df)} phòng ban -> {departments_path}")
        
        print("\n✓ Hoàn thành! Dữ liệu đã được lưu.")
        print("\nThống kê:")
        print(f"- Employees: {len(employees_df)}")
        print(f"- Products: {len(products_df)}")
        print(f"- Sales: {len(sales_df)}")
        print(f"- Departments: {len(departments_df)}")
        
        return {
            "employees": employees_df,
            "products": products_df,
            "sales": sales_df,
            "departments": departments_df
        }

