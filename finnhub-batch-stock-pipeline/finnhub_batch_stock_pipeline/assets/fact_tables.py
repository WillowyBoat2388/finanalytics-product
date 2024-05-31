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

    for cols in series.columns:
        if cols == "date":
            series =  series.withColumn(cols, F.col(cols).cast(DateType()))
        elif cols == "symbol":
            series = series.withColumn(cols, F.cast(StringType(), F.col(cols)))
        else:
            series = series.withColumn(cols, F.col(f"`{cols}`").cast(FloatType()))
    for cols in metric.columns:
        if cols == "symbol":
            metric = metric.withColumn(cols, F.cast(StringType(), F.col(cols)))
        else:
            metric = metric.withColumn(cols, F.col(cols).cast(FloatType())) 
    

    # context.log.info(metric.schema.simpleString)
    # context.log.info(series.schema.simpleString)

    metric_fact_cols = list(metric.columns)

    metric_fact_cols.remove("symbol")
    metric_fact_cols.append("symbol")

    metric_fact = metric

    metric_fact = metric_fact.select(*metric_fact_cols)

    series_fact = series

    metric_fact_wrh = metric_fact

    series_fact_wrh = series_fact

    metric_fact_lake = metric._collect_as_arrow()
    metric_fact_lake = pa.Table.from_batches(metric_fact_lake)

    series_fact_lake = series._collect_as_arrow()
    series_fact_lake = pa.Table.from_batches(series_fact_lake)

    return metric_fact_lake, series_fact_lake, metric_fact_wrh, series_fact_wrh