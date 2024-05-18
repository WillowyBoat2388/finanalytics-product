from dagster import define_asset_job, AssetSelection, job, load_assets_from_modules
from ..assets.spark_transformations import stock_tables, create_stock_tables, pyspark_operator_3
from ..assets.raw import finnhub_stocks
from ..assets.raw_io_managers_use import finnhub_stocks_US
from ..assets.graph_raw import finnhub_US_stocks



@job
def finnhub_transform_job():
    stocks = finnhub_US_stocks()
    dynamic_table = stock_tables(stocks)
    collected = dynamic_table.map(create_stock_tables)