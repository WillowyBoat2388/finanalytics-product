from dagster import (asset, 
                Output, 
                AssetExecutionContext, 
                DynamicPartitionsDefinition, 
                DagsterInstance)
from dagster_aws.s3 import S3Resource
from ..resources import MyConnectionResource
from typing import Dict, List

import pandas as pd
import datetime as dt
import time

dynamic_partitions_def = DynamicPartitionsDefinition(name="stocks")

@asset(
    group_name="raw",
)
def finnhub_stocks(context, my_conn: MyConnectionResource) -> List:
    """
        Asset for loading data from FinnHub API to get stock symbols
    """

    endpoint = "stock/symbol?exchange=US&currency=USD&securityType=Common Stock"
    stocks = my_conn.request(endpoint)

    stock_list = [i['symbol'] for i in stocks]
    stock_list = sorted(stock_list)

    context.add_output_metadata(
        metadata={
            "num_records": len(stock_list),
        }
    )

    context.instance.add_dynamic_partitions("stocks", stock_list)

    return stock_list

@asset(
    group_name="raw",
    io_manager_key="s3_json_io_manager",
    deps= [finnhub_stocks],
    partitions_def = dynamic_partitions_def
)
def finnhub_stocks_US(context: AssetExecutionContext, my_conn: MyConnectionResource, s3: S3Resource) -> Dict:
    """
        Dagster asset for loading data using upstream in-memory list from Finnhub API 
        specifying common stocks in the US stock exchange
    """
    stock = context.asset_partition_key_for_output()
    # for stock in upstream:
    endpoint = f"stock/metric?symbol={stock}&metric=all"

    stock_data = my_conn.request(endpoint)
    return stock_data

        

    
