# import json
# import os
# from typing import Tuple
# from ..resources import MyConnectionResource
# import requests
# from dagster import AssetSelection, RunRequest, SensorResult, SkipReason, sensor, AssetKey

# from ..assets import assets


# def semver_tuple(release: str) -> Tuple[int, ...]:
#     return tuple(map(int, release.split(".")))


# @sensor(asset_selection=AssetSelection.keys("finnhub_stocks_US"))
# def stocks_sensor(context, my_conn: MyConnectionResource):
#     """Sensor that triggers a run whenever the latest version of an asset is released.

#     When we find one, add it to the set of partitions and run the pipeline on it.
#     """
#     runs_to_request = []
#     latest_tracked_stocks = context.cursor

#     endpoints = "stock/symbol?exchange=US&currency=USD&securityType=Common Stock"
    
#     stocks = my_conn.request(endpoints)

#     stock_list = [i['symbol'] for i in stocks]
#     stock_list = sorted(stock_list)

#     if latest_tracked_stocks is None:
#         new_stocks = stock_list
#     else:
#         new_stocks = [
#             stock
#             for stock in stock_list
#             if stock not in latest_tracked_stocks
#         ]


#     all_stocks = latest_tracked_stocks
    
#     all_stocks += str(new_stocks)
#     context.log.error(all_stocks)

#     if len(new_stocks) > 0:
#         partitions_to_add = new_stocks
#         context.log.info(f"Requesting to add partitions: {partitions_to_add}")

#         # We only launch a run for the latest release, to avoid unexpected large numbers of runs the
#         # first time the sensor turns on. This means that you might need to manually backfill earlier
#         # releases.
#         for partition in partitions_to_add:
#                 runs_to_request.append(RunRequest(
#                         run_key=f"finnhub_stocks_US_{partition}",
#                         asset_selection=[AssetKey('finnhub_stocks_US')],
#                         partition_key = partition,
#                     ))
        
#         return SensorResult(
#             run_requests= runs_to_request, #[RunRequest(partition_key=new_releases[-1])],
#             cursor=all_stocks,
#             dynamic_partitions_requests=[
#                 assets.dynamic_partitions_def.build_add_request(partitions_to_add)
#             ],
#         )
#     else:
#         return SkipReason("No new stocks")