from setuptools import setup, find_packages

setup(
    name="ecommerce_bigdata_pipeline",
    version="1.0.0",
    description="AWS Big Data E-commerce pipeline with PySpark, SCD, and Airflow",
    author="Antigravity",
    packages=find_packages(exclude=["tests", "dags", "scripts"]),
    install_requires=[
        "pyspark>=3.3.0",
        "pandas>=1.5.0",
        "numpy>=1.20.0",
        "pyyaml>=6.0",
        "boto3>=1.26.0",
    ],
    python_requires=">=3.8",
)
