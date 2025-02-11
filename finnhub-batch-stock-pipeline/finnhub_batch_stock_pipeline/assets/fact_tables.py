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
           "quarterly_metric": AssetIn(key = AssetKey("quarterly_metric"), input_manager_key="s3_prqt_io_manager"),
           "annual_metric": AssetIn(key = AssetKey("annual_metric"), input_manager_key="s3_prqt_io_manager")},
    outs = {"metric_fact_lake": AssetOut(is_required=False, group_name= "fact_lakehouse", metadata={ "mode": "append"}, io_manager_key="delta_lake_arrow_io_manager"),
            "quarterly_metric_fact_lake": AssetOut(is_required=False, group_name= "fact_lakehouse", metadata={ "mode": "append"}, io_manager_key="delta_lake_arrow_io_manager"),
            "annual_metric_fact_lake": AssetOut(is_required=False, group_name= "fact_lakehouse", metadata={ "mode": "append"}, io_manager_key="delta_lake_arrow_io_manager"),
            "metric_fact_wrh": AssetOut(is_required=False, group_name= "fact_warehouse", io_manager_key="warehouse_io_manager"),
            "quarterly_metric_fact_wrh": AssetOut(is_required=False, group_name= "fact_warehouse", io_manager_key="warehouse_io_manager"),
            "annual_metric_fact_wrh": AssetOut(is_required=False, group_name= "fact_warehouse", io_manager_key="warehouse_io_manager"),
    },
    internal_asset_deps={
        "metric_fact_lake": {AssetKey(["metric"])},
        "quarterly_metric_fact_lake": {AssetKey(["quarterly_metric"])},
        "annual_metric_fact_lake": {AssetKey(["annual_metric"])},
        "metric_fact_wrh": {AssetKey(["metric"])},
        "quarterly_metric_fact_wrh": {AssetKey(["quarterly_metric"])},
        "annual_metric_fact_wrh": {AssetKey(["annual_metric"])}
    },
    can_subset=True
)
def fact_tables(context, metric:DataFrame, quarterly_metric:DataFrame, annual_metric:DataFrame) -> tuple[pa.Table, pa.Table, pa.Table, DataFrame, DataFrame, DataFrame]:
    """
    Multi asset for defining assets which populate data warehouse and data lake while incorporating 
    quality checks for datasets.
    """

    # Iterate over each column in the series DataFrame
    for cols in quarterly_metric.columns:
        if cols == "date":
            # Cast 'date' column to DateType
            quarterly_metric = quarterly_metric.withColumn(cols, F.col(cols).cast(DateType()))
        elif cols == "symbol":
            # Cast 'symbol' column to StringType
            quarterly_metric = quarterly_metric.withColumn(cols, F.cast(StringType(), F.col(cols)))
        else:
            # Cast all other columns to FloatType
            quarterly_metric = quarterly_metric.withColumn(cols.split(".")[-1], F.col(f"`{cols}`").cast(FloatType()))

    # Iterate over each column in the series DataFrame
    for cols in annual_metric.columns:
        if cols == "date":
            # Cast 'date' column to DateType
            annual_metric = annual_metric.withColumn(cols, F.col(cols).cast(DateType()))
        elif cols == "symbol":
            # Cast 'symbol' column to StringType
            annual_metric = annual_metric.withColumn(cols, F.cast(StringType(), F.col(cols)))
        else:
            # Cast all other columns to FloatType
            annual_metric = annual_metric.withColumn(cols.split(".")[-1], F.col(f"`{cols}`").cast(FloatType()))

    # Iterate over each column in the metric DataFrame
    for cols in metric.columns:
        if cols == "symbol":
            # Cast 'symbol' column to StringType
            metric = metric.withColumn(cols, F.cast(StringType(), F.col(cols)))
        else:
            # Cast all other columns to FloatType
            metric = metric.withColumn(cols, F.col(cols).cast(FloatType()))


    # Assign metric DataFrame to metric_fact
    metric_fact = metric

    # Assign series DataFrame to series_fact
    quarterly_metric_fact = quarterly_metric
    annual_metric_fact = annual_metric

    # Create warehouse versions of the DataFrames
    metric_fact_wrh = metric_fact
    quarterly_metric_fact_wrh = quarterly_metric_fact
    annual_metric_fact_wrh = annual_metric_fact

    # Convert metric DataFrame to an Arrow Table for the data lake
    metric_fact_lake = metric._collect_as_arrow()
    metric_fact_lake = pa.Table.from_batches(metric_fact_lake)

    # Convert series DataFrame to an Arrow Table for the data lake
    quarterly_metric_fact_lake = quarterly_metric._collect_as_arrow()
    quarterly_metric_fact_lake = pa.Table.from_batches(quarterly_metric_fact_lake)
    annual_metric_fact_lake = annual_metric._collect_as_arrow()
    annual_metric_fact_lake = pa.Table.from_batches(annual_metric_fact_lake)

    # Return the DataFrames for both warehouse and data lake
    return metric_fact_lake, quarterly_metric_fact_lake, annual_metric_fact_lake, metric_fact_wrh, quarterly_metric_fact_wrh, annual_metric_fact_wrh
