from dagster import (graph_asset,
                op,
                Out,
                MetadataValue
            )
from ..resources import MyConnectionResource
from typing import Dict
from datetime import datetime as dt
import time

@op
def import_stocks_def(context, my_conn: MyConnectionResource):
    """
        Operation for loading data from FinnHub API to get stock symbols
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
    return stock_list


@op(
    out=Out(metadata= {"date": dt.now().strftime("%Y-%m-%d")}, io_manager_key="s3_json_io_manager"),
    )
def import_stocks_data(input_list, my_conn: MyConnectionResource):
    json_object = {}
    count = 0
    symbol_count = 0
    for stock in input_list:
        symbol_count += 1
        if count == 60:
            count = 0
            time.sleep(65)
        
        # for stock in upstream:
        endpoint = f"stock/metric?symbol={stock}&metric=all"

        stock_data = my_conn.request(endpoint)

        json_object[stock] = stock_data

        if symbol_count == 100:
            break
        count += 1
    
    return json_object
    
    
@graph_asset(
    group_name="raw",
    # auto_materialize_policy=AutoMaterializePolicy.eager
)
def finnhub_US_stocks() -> Dict:
    """
        Dagster asset for loading data using upstream in-memory list from Finnhub API 
        specifying common stocks in the US stock exchange
    """
    return import_stocks_data(import_stocks_def())