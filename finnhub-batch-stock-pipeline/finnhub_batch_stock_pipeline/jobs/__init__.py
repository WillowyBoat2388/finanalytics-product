from dagster import define_asset_job, AssetSelection, job, load_assets_from_modules, AutoMaterializePolicy
from ..assets import graph_raw, spark_transformations

# load assets from module name and using dagster's inbuilt assetselection method
warehouse_tables = AssetSelection.assets("metric_fact_wrh", "series_fact_wrh")
lake_tables = AssetSelection.assets("metric_fact_lake", "series_fact_lake")
stock_retrieval_assets = load_assets_from_modules(modules=[graph_raw])
pyspark_transformations = load_assets_from_modules(modules=[spark_transformations])


# job to trigger execution of assets which update S3 data lake
lake_update_job = define_asset_job(
    name="lake_update_job",
    selection=lake_tables
)
# job to trigger execution of assets which update specified data warehouse
warehouse_update_job = define_asset_job(
    name="warehouse_update_job",
    selection=warehouse_tables
)
# job to trigger execution of assets which retrieve stock selection data
stock_retrieval_job = define_asset_job(
    name="stock_retrieval_job",
    selection=stock_retrieval_assets
)
# job to trigger execution of Spark jobs which transform stock dataset
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