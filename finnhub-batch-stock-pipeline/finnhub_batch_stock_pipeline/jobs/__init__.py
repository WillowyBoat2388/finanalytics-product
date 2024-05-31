from dagster import define_asset_job, AssetSelection, job, load_assets_from_modules, AutoMaterializePolicy
from ..assets import graph_raw, spark_transformations


warehouse_tables = AssetSelection.assets("metric_fact_wrh", "series_fact_wrh")
lake_tables = AssetSelection.assets("metric_fact_lake", "series_fact_lake")
stock_retrieval_assets = load_assets_from_modules(modules=[graph_raw])
pyspark_transformations = load_assets_from_modules(modules=[spark_transformations])



lake_update_job = define_asset_job(
    name="lake_update_job",
    selection=lake_tables
)

warehouse_update_job = define_asset_job(
    name="warehouse_update_job",
    selection=warehouse_tables
)

stock_retrieval_job = define_asset_job(
    name="stock_retrieval_job",
    selection=stock_retrieval_assets
)

spark_transformation_job = define_asset_job(
    name="spark_transformation_job",
    selection=pyspark_transformations
)


# @job
# def stock_retrieval_job():
#     stocks = finnhub_US_stocks()
#     spark_operator()


# @job
# def lake_update_job():
#     fact_tables._selected_asset_keys(["metric_fact_lake", "series_fact_lake"])
# @job
# def warehouse_update_job():
#     fact_tables._selected_asset_keys(["metric_fact_wrh", "series_fact_wrh"])