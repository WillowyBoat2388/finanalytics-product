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
    # Finnhub API endpoint from which stock symbols are to be retrieved
    endpoint = "stock/symbol?exchange=US&currency=USD&securityType=Common Stock"
    # Calling the conenction resource which specifies the api token to use for retrieval
    stocks = my_conn.request(endpoint)

    # extract symbol name from retrieved api call and stores it in a list
    stock_list = [i['symbol'] for i in stocks]
    # sorts retrieved symbol names
    stock_list = sorted(stock_list)

    # pass total number of stocks retrieved to dagster webserver ouput
    context.add_output_metadata(
        metadata={
            "num_records": len(stock_list),
        }
    )
    # returns stock list to be saved in-memory for use in downstream operations 
    return stock_list


@op(
    out=Out(metadata= {"date": dt.now().strftime("%Y-%m-%d")}, io_manager_key="s3_json_io_manager"),
    )
def import_stocks_data(input_list, my_conn: MyConnectionResource):
    """
    Dagster op which automates the retrieval of stock data using their retrieved symbols
    and then concatenating into a list for downstream processing.
    """
    # initialize new dictionary which will hold all data points for downstream processing
    json_object = {}
    # initialize count to zero for managing api rate call limits
    count = 0

    symbol_count = 0

    # begin iteration over retrieved stock symbols
    for stock in input_list:
        symbol_count += 1
        # condition to check if rate limit has been achieved
        if count == 60:
            count = 0
            time.sleep(65)
        
            # Finnhub API endpoint from which stock symbols data are to be retrieved
            endpoint = f"stock/metric?symbol={stock}&metric=all"

            # Calling the connection resource which specifies the api token to use for retrieval
            stock_data = my_conn.request(endpoint)
            # store stock data in dictionary using stock symbol as key
            json_object[stock] = stock_data

        if symbol_count == 100:
            break
        # increase counter
        count += 1
    # return nested json dictionary to S3 using json IO manager
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