from dagster import (asset, 
                multi_asset,
                AssetOut
            )
from dagster_aws.s3 import S3Resource
from ..resources import MyConnectionResource
from typing import Tuple, Dict

import io
import json
import pandas as pd
from datetime import datetime as dt
import time

bucket = "dagster-api"

@multi_asset(
    group_name="raw_resource",
    outs={
            "metrics_paths": AssetOut(),
            "series_paths": AssetOut(),
        },
)
def finnhub_US_stocks_tables(finnhub_stocks: list, my_conn: MyConnectionResource, s3: S3Resource) -> Tuple[Dict, Dict]:
    """
        Dagster asset for loading data using upstream in-memory list from Finnhub API 
        specifying common stocks in the US stock exchange. And saving to an S3 bucket using the Dagster S3 resource
    """
    stock_metric_paths = {}
    stock_series_paths = {}
    count = 0
    for stock in finnhub_stocks:
        date = dt.now()
        date = date.strftime("%Y-%m-%d")

        if count == 60:
            time.sleep(61)
            count = 0
        # for stock in upstream:
        endpoint = f"stock/metric?symbol={stock}&metric=all"

        stock_data = my_conn.request(endpoint)

        dfs = {}
        for keys, values in stock_data.items():
            
            if keys == 'metric':
                # Normalize each inner dictionary and store the result in a DataFrame
                dft = pd.json_normalize(values)
                dfs[keys] = dft
            elif keys == 'series':
                df_dict = {}
                for readings in values:
                    if readings:
                        dft = {}
                        for key, value in values[readings].items():
                            if value:
                                # Normalize each inner dictionary and store the result in a DataFrame
                                dft[key] = pd.json_normalize(value)
                        df_dict[readings] = dft
                dfs[keys] = df_dict
            else:
                dfs[keys] = values
            
        series = dfs["series"]
        df_list_o = []
        for readings, dft in series.items():
            df_list = None
            i = 0
            for key, df in dft.items():
                df.set_index('period', inplace=True)
                if i == 0:
                    df_list = df
                    df_list.rename(columns={'v': f'{readings}_{key}'}, inplace=True)
                else:
                    df_list = df_list.join(df, how= 'outer', sort= True,)
                    df_list.rename(columns={'v': f'{readings}_{key}'}, inplace=True)
                i += 1

            df_list_o.append(df_list)    
                    
        try:
            dfs['series'] = pd.concat(df_list_o, axis = 1)
            dfs['series'].reset_index(inplace=True)
        
            final_df_series = dfs['series']
            final_df_metric = dfs['metric']
            for key, value in dfs.items():
                if key != 'series' and key != 'metric':
                    key_series = pd.Series([value] * len(dfs['series']), name = key)
                    key_metric = pd.Series([value] * len(dfs['metric']), name = key)
                    final_df_series = pd.concat([final_df_series, key_series], axis= 1)
                    final_df_metric = pd.concat([final_df_metric, key_metric], axis= 1)
        except ValueError as e:
            print("Value Error: ", str(e), "empty series:", series, f"of {stock}")
            continue
        metric_path = f"raw/metrics/{stock}_{date}.parquet"
        series_path = f"raw/series/{stock}_{date}.parquet"

        metric_parquet_buffer = io.BytesIO()
        final_df_metric.to_parquet(metric_parquet_buffer, engine="pyarrow")
        metric_parquet_buffer.seek(0)

        series_parquet_buffer = io.BytesIO()
        final_df_metric.to_parquet(series_parquet_buffer, engine="pyarrow")
        series_parquet_buffer.seek(0)


        stock_metric_paths[stock] = metric_path
        stock_series_paths[stock] = series_path
        # s3.get_object(Bucket=bucket, Key=path.as_posix())["Body"].read()
        s3.get_client().upload_fileobj(metric_parquet_buffer, Bucket=bucket, Key=metric_path)
        s3.get_client().upload_fileobj(series_parquet_buffer, Bucket=bucket, Key=series_path)
        count += 1
        if count == 500: break

    return stock_metric_paths, stock_series_paths

@asset(
    group_name="raw_resource",
)
def finnhub_US_stocks_json(finnhub_stocks: list, my_conn: MyConnectionResource, s3: S3Resource) -> Dict:
    """
        Dagster asset for loading data using upstream in-memory list from Finnhub API 
        specifying common stocks in the US stock exchange to a json file. And saving to an S3 bucket using the Dagster S3 resource.
    """
    stock_paths = {}
    count = 0
    for stock in finnhub_stocks:
        date = dt.now()
        date = date.strftime("%Y-%m-%d")

        if count == 60:
            time.sleep(61)
            count = 0
        # for stock in upstream:
        endpoint = f"stock/metric?symbol={stock}&metric=all"

        stock_data = my_conn.request(endpoint)

        path = f"raw/metrics/{stock}_{date}.json"

        json_object = json.dumps(stock_data)
        json_buffer = io.BytesIO(json_object)
        # series_parquet_buffer.seek(0)


        stock_paths[stock] = path
        s3.get_client().upload_fileobj(json_object, Bucket=bucket, Key=path)
        count += 1
        if count == 500: break

    return stock_paths
