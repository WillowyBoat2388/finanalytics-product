from dagster import asset
from ..resources import MyConnectionResource
from typing import List



@asset(
    group_name="raw_og",
    compute_kind="Finnhub_API",
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

    # context.instance.add_dynamic_partitions("stocks", stock_list)

    return stock_list
        