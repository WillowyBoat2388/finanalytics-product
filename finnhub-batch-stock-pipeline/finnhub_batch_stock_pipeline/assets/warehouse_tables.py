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

date = dt.now().strftime("%Y-%m-%d")

# metric_src = SourceAsset(key=[date, "metric"], group_name="obj_storage")
# series_src = SourceAsset(key=[date, "series"], group_name="obj_storage")



@multi_asset(
    group_name = "dim",
    ins = {"metric": AssetIn(key = AssetKey("metric"), input_manager_key="s3_prqt_io_manager"),
           "series": AssetIn(key = AssetKey("series"), input_manager_key="s3_prqt_io_manager")},
    outs = {"metric_dim": AssetOut(metadata={ "mode": "append"}, io_manager_key="delta_lake_arrow_io_manager"),
            "series_dim": AssetOut(metadata={ "mode": "append"}, io_manager_key="delta_lake_arrow_io_manager"),
            # "metric_dim_wrh": AssetOut(metadata={ "mode": "append"}, io_manager_key="warehouse_io_manager"),
            # "series_dim_wrh": AssetOut(metadata={ "mode": "append"}, io_manager_key="warehouse_io_manager")
    },
    internal_asset_deps={
        "metric_dim": {AssetKey(["metric"])},
        "series_dim": {AssetKey(["series"])},
        # "metric_dim_wrh": {AssetKey(["metric"])},
        # "series_dim_wrh": {AssetKey(["series"])}
    }
)
def dimension_tables(metric, series) -> tuple[pa.Table, pa.Table]:#, DataFrame, DataFrame]:
    

    metric_dim = metric._collect_as_arrow()
    metric_dim = pa.Table.from_batches(metric_dim)
    metric_dim_wrh = metric.toPandas()

    series_dim = series._collect_as_arrow()
    series_dim = pa.Table.from_batches(series_dim)
    series_dim_wrh = series.toPandas()
    
    return metric_dim, series_dim#, metric, series


# @multi_asset(
#     group_name= "dim",
#     ins = {"metric": AssetIn(key = AssetKey("metric"), input_manager_key="s3_prqt_io_manager"),
#            "series": AssetIn(key = AssetKey("series"), input_manager_key="s3_prqt_io_manager")},
#     outs = {"metric_visual": AssetOut(io_manager_key="warehouse_io_manager"),
#             "series_visual": AssetOut(io_manager_key="warehouse_io_manager")
#     },
#     internal_asset_deps={
#         "metric_dim": {AssetKey(["metric"])},
#         "series_dim": {AssetKey(["series"])},
#     }
# )
# def dimension_wrh_tables(metric_dim, series_dim) -> tuple[pd.DataFrame, pd.DataFrame]:
    

#     metric_dim = metric.toPandas()

#     series_dim = series.toPandas()

#     return metric_dim, series_dim




