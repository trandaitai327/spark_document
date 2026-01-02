"""
Setup script for PySpark Sales Analysis Project
"""
from setuptools import setup, find_packages
from pathlib import Path

# Đọc README
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text(encoding="utf-8") if readme_file.exists() else ""

# Đọc requirements
requirements_file = Path(__file__).parent / "requirements.txt"
if requirements_file.exists():
    with open(requirements_file, "r", encoding="utf-8") as f:
        requirements = [line.strip() for line in f if line.strip() and not line.startswith("#")]
else:
    requirements = ["pyspark>=3.5.0", "pandas>=2.0.0"]

setup(
    name="pyspark-sales-analysis",
    version="1.0.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A comprehensive PySpark project for analyzing sales data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/pyspark-sales-analysis",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "pyspark-generate-data=pyspark_project.data.generator:main",
            "pyspark-run-analysis=pyspark_project.analysis.run:main",
        ],
    },
)

