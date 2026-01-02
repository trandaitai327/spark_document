"""
Script tạo dữ liệu mẫu cho project PySpark
"""
import pandas as pd
import random
import os
from datetime import datetime, timedelta

def generate_employees(n=100):
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

def generate_products(n=50):
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

def generate_sales(n=10000):
    """Tạo dữ liệu bán hàng"""
    data = []
    start_date = datetime.now() - timedelta(days=365)
    
    for i in range(1, n + 1):
        sale_date = start_date + timedelta(days=random.randint(0, 365))
        quantity = random.randint(1, 10)
        product_id = random.randint(1, 50)
        employee_id = random.randint(1, 100)
        
        # Lấy giá từ products (giả sử giá trung bình 100)
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

def generate_departments():
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

def main():
    """Hàm chính để tạo tất cả dữ liệu"""
    print("Bắt đầu tạo dữ liệu mẫu...")
    
    # Tạo thư mục nếu chưa tồn tại
    os.makedirs("data/raw", exist_ok=True)
    
    # Tạo dữ liệu
    print("Tạo dữ liệu employees...")
    employees_df = generate_employees(100)
    employees_df.to_csv("data/raw/employees.csv", index=False)
    print(f"✓ Đã tạo {len(employees_df)} nhân viên")
    
    print("Tạo dữ liệu products...")
    products_df = generate_products(50)
    products_df.to_csv("data/raw/products.csv", index=False)
    print(f"✓ Đã tạo {len(products_df)} sản phẩm")
    
    print("Tạo dữ liệu sales...")
    sales_df = generate_sales(10000)
    sales_df.to_csv("data/raw/sales.csv", index=False)
    print(f"✓ Đã tạo {len(sales_df)} giao dịch bán hàng")
    
    print("Tạo dữ liệu departments...")
    departments_df = generate_departments()
    departments_df.to_csv("data/raw/departments.csv", index=False)
    print(f"✓ Đã tạo {len(departments_df)} phòng ban")
    
    print("\n✓ Hoàn thành! Dữ liệu đã được lưu trong thư mục data/raw/")
    print("\nThống kê:")
    print(f"- Employees: {len(employees_df)}")
    print(f"- Products: {len(products_df)}")
    print(f"- Sales: {len(sales_df)}")
    print(f"- Departments: {len(departments_df)}")

if __name__ == "__main__":
    main()

