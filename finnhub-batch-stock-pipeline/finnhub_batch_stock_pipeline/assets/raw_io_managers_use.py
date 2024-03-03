from dagster import (asset, 
                multi_asset,
                AssetOut,
                MetadataValue
            )
from ..resources import MyConnectionResource
from typing import Dict
import time
from datetime import datetime as dt

# from .raw import finnhub_stocks

import pandas as pd

@asset(
    group_name="raw_io_manager",
    io_manager_key="s3_json_io_manager",
    metadata={"date": dt.now().strftime("%Y-%m-%d")},
)
def finnhub_stocks_US(context, finnhub_stocks: list, my_conn: MyConnectionResource) -> Dict:
    """
        Dagster asset for loading data using upstream in-memory list from Finnhub API 
        specifying common stocks in the US stock exchange
    """
    date = dt.now().strftime("%Y-%m-%d")
    json_object = {}
    count = 0
    symbol_count = 0
    for stock in finnhub_stocks:
        symbol_count += 1
        if count == 60:
            count = 0
            time.sleep(65)
        
        # for stock in upstream:
        endpoint = f"stock/metric?symbol={stock}&metric=all"

        stock_data = my_conn.request(endpoint)

        json_object[stock] = stock_data

        if symbol_count == 10:
            break
        count += 1

        
    context.add_output_metadata({"date": date})
    return json_object

@asset(
    group_name="raw_fancy_io_manager",
    io_manager_key="s3_fancy_json_io_manager",
)
def finnhub_stocks_US_gen(finnhub_stocks: list, my_conn: MyConnectionResource):
    """
        Dagster asset for loading data using upstream in-memory list from Finnhub API 
        specifying common stocks in the US stock exchange. This asset returns a generator function 
        which the io manager then calls and runs a loop over to generate each file 
        that then get saved in the storage location.
    """
    def generator_fn():
        for stock in finnhub_stocks:
            # for stock in upstream:
            endpoint = f"stock/metric?symbol={stock}&metric=all"

            stock_data = my_conn.request(endpoint)
            yield(stock_data)

    return generator_fn

@asset(
    group_name="raw_fancy_io_manager",
    # deps= [finnhub_stocks],
    # outs={
    #         "metrics": AssetOut(io_manager_key="s3_fancy_prqt_io_manager"),
    #         "series": AssetOut(io_manager_key="s3_fancy_prqt_io_manager"),
    #     },
)
def finnhub_stocks_US_multi_gen(finnhub_stocks: list, my_conn: MyConnectionResource):
    """
        Dagster asset for loading data using upstream in-memory list from Finnhub API 
        specifying common stocks in the US stock exchange. This asset returns a generator function 
        which the io manager then calls and runs a loop over to generate each file 
        that then get saved in the storage location. This generator generates two files per stock.
        One is metrics data (which includes all available financial metrics), another one is time series data.
    """
    def generator_fn():
        for stock in finnhub_stocks:
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

            yield(final_df_series, final_df_metric)

    return generator_fn

        

@multi_asset(
    group_name="raw_io_manager_dict",
    outs={
            "metrics": AssetOut(io_manager_key="s3_prqt_io_manager"),
            "series": AssetOut(io_manager_key="s3_prqt_io_manager"),
        },
)
def finnhub_stocks_US_dict(finnhub_stocks: list, my_conn: MyConnectionResource) -> tuple[Dict, Dict]:
    """
        Dagster asset for loading data using upstream in-memory list from Finnhub API 
        specifying common stocks in the US stock exchange. This asset returns two dictionaries 
        which the io manager calls and runs a loop over each to generate a file 
        that then gets saved in the storage location.
    """
    metrics_dict = {}
    series_dict = {}
    for stock in finnhub_stocks:
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
            metrics_dict[stock] = final_df_metric
            series_dict[stock] = final_df_series

        except ValueError as e:
            print("Value Error: ", str(e), "empty series:", series, f"of {stock}")
            continue

    return metrics_dict, series_dict
        
