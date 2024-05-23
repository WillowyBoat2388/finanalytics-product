from dagster import (asset,
                     multi_asset,
                     AssetIn,
                AssetOut,
                SourceAsset,
                AssetKey,
                MetadataValue
            )

from typing import List, Any
from datetime import datetime as dt
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as F

date = dt.now().strftime("%Y-%m-%d")

# metric_src = SourceAsset(key=[date, "metric"], group_name="obj_storage")
# series_src = SourceAsset(key=[date, "series"], group_name="obj_storage")



@multi_asset(
    group_name = "fact_lake",
    ins = {"metric": AssetIn(key = AssetKey("metric"), input_manager_key="s3_prqt_io_manager"),
           "series": AssetIn(key = AssetKey("series"), input_manager_key="s3_prqt_io_manager")},
    outs = {"metric_fact": AssetOut(metadata={ "mode": "append"}, io_manager_key="delta_lake_arrow_io_manager"),
            "series_fact": AssetOut(metadata={ "mode": "append"}, io_manager_key="delta_lake_arrow_io_manager")
    },
    internal_asset_deps={
        "metric_fact": {AssetKey(["metric"])},
        "series_fact": {AssetKey(["series"])}
    }
)
def fact_tables(metric, series) -> tuple[pa.Table, pa.Table]:
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


    metric_fact = metric._collect_as_arrow()
    metric_fact = pa.Table.from_batches(metric_fact)

    series_fact = series._collect_as_arrow()
    series_fact = pa.Table.from_batches(series_fact)
    
    return metric_fact, series_fact


@multi_asset(
    group_name= "fact_warehouse",
    ins = {"metric": AssetIn(key = AssetKey("metric"), input_manager_key="s3_prqt_io_manager"),
           "series": AssetIn(key = AssetKey("series"), input_manager_key="s3_prqt_io_manager")},
    outs = {"metric_fact_wrh": AssetOut(io_manager_key="warehouse_io_manager"),
            "series_fact_wrh": AssetOut(io_manager_key="warehouse_io_manager")
    },
    internal_asset_deps={
        "metric_fact_wrh": {AssetKey(["metric"])},
        "series_fact_wrh": {AssetKey(["series"])},
    }
)
def fact_wrh_tables(metric, series) -> tuple[DataFrame, DataFrame]:

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

    metric_fact_wrh = metric

    series_fact_wrh = series

    return metric_fact_wrh, series_fact_wrh




