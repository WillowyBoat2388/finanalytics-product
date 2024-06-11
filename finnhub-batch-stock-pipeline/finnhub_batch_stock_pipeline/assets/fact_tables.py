from dagster import (multi_asset,
                     AssetIn,
                AssetOut,
                SourceAsset,
                AssetKey,
            )
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as F

# metric_src = SourceAsset(key=[date, "metric"], group_name="obj_storage")
# series_src = SourceAsset(key=[date, "series"], group_name="obj_storage")

@multi_asset(
    ins = {"metric": AssetIn(key = AssetKey("metric"), input_manager_key="s3_prqt_io_manager"),
           "series": AssetIn(key = AssetKey("series"), input_manager_key="s3_prqt_io_manager")},
    outs = {"metric_fact_lake": AssetOut(is_required=False, group_name= "fact_lakehouse", metadata={ "mode": "append"}, io_manager_key="delta_lake_arrow_io_manager"),
            "series_fact_lake": AssetOut(is_required=False, group_name= "fact_lakehouse", metadata={ "mode": "append"}, io_manager_key="delta_lake_arrow_io_manager"),
            "metric_fact_wrh": AssetOut(is_required=False, group_name= "fact_warehouse", io_manager_key="warehouse_io_manager"),
            "series_fact_wrh": AssetOut(is_required=False, group_name= "fact_warehouse", io_manager_key="warehouse_io_manager"),
    },
    internal_asset_deps={
        "metric_fact_lake": {AssetKey(["metric"])},
        "series_fact_lake": {AssetKey(["series"])},
        "metric_fact_wrh": {AssetKey(["metric"])},
        "series_fact_wrh": {AssetKey(["series"])},
    },
    can_subset=True
)
def fact_tables(context, metric:DataFrame, series:DataFrame) -> tuple[pa.Table, pa.Table, DataFrame, DataFrame]:
    """
    Multi asset for defining assets which populate data warehouse and data lake while incorporating 
    quality checks for datasets.
    """

    # Iterate over each column in the series DataFrame
    for cols in series.columns:
        if cols == "date":
            # Cast 'date' column to DateType
            series = series.withColumn(cols, F.col(cols).cast(DateType()))
        elif cols == "symbol":
            # Cast 'symbol' column to StringType
            series = series.withColumn(cols, F.cast(StringType(), F.col(cols)))
        else:
            # Cast all other columns to FloatType
            series = series.withColumn(cols, F.col(f"`{cols}`").cast(FloatType()))

    # Iterate over each column in the metric DataFrame
    for cols in metric.columns:
        if cols == "symbol":
            # Cast 'symbol' column to StringType
            metric = metric.withColumn(cols, F.cast(StringType(), F.col(cols)))
        else:
            # Cast all other columns to FloatType
            metric = metric.withColumn(cols, F.col(cols).cast(FloatType()))

    # Create a list of metric DataFrame columns
    metric_fact_cols = list(metric.columns)

    # Move 'symbol' column to the end of the list
    metric_fact_cols.remove("symbol")
    metric_fact_cols.append("symbol")

    # Assign metric DataFrame to metric_fact
    metric_fact = metric

    # Select columns in the specified order (with 'symbol' at the end)
    metric_fact = metric_fact.select(*metric_fact_cols)

    # Assign series DataFrame to series_fact
    series_fact = series

    # Create warehouse versions of the DataFrames
    metric_fact_wrh = metric_fact
    series_fact_wrh = series_fact

    # Convert metric DataFrame to an Arrow Table for the data lake
    metric_fact_lake = metric._collect_as_arrow()
    metric_fact_lake = pa.Table.from_batches(metric_fact_lake)

    # Convert series DataFrame to an Arrow Table for the data lake
    series_fact_lake = series._collect_as_arrow()
    series_fact_lake = pa.Table.from_batches(series_fact_lake)

    # Return the DataFrames for both warehouse and data lake
    return metric_fact_lake, series_fact_lake, metric_fact_wrh, series_fact_wrh
