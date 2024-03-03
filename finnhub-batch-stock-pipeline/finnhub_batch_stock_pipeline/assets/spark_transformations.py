from dagster import (asset
            )
from typing import Dict, List

import io
import json
import pandas as pd
from datetime import datetime as dt
import time

@asset(
    group_name= "resource_transformations"
)
def pyspark_operator(metrics_paths: Dict, series_paths: Dict):
    """
    
    """
    # s3.get_object(Bucket=bucket, Key=path.as_posix())["Body"].read()
    df = None
    pass

@asset(
    group_name = "io_transformations",
)
def pyspark_operator_2(finnhub_stocks_US: List):
    """
    I don't even know at this point
    """
    for stock_data in finnhub_stocks_US:
        print(stock_data)
        break
    
    pass

@asset(
    group_name = "io_transformations",
)
def pyspark_operator_3(finnhub_US_stocks: List):
    """
    I don't even know at this point
    """
    for stock_data in finnhub_US_stocks:
        print(stock_data)
        break
    
    pass