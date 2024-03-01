from setuptools import find_packages, setup

setup(
    name="finnhub_batch_stock_pipeline",
    packages=find_packages(exclude=["finnhub_batch_stock_pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pyspark",
        "pandas",
        "requests",
        "pyarrow"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
