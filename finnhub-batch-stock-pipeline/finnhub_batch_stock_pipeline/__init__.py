from dagster import (Definitions, 
                    load_assets_from_modules, 
                    EnvVar, 
                    FilesystemIOManager)
from dagster_aws.s3 import S3PickleIOManager
from .iomanagers import (s3_json_io_manager as s3j, 
                         s3_parquet_io_manager as s3p,
                         s3_fancy_parquet_io_manager as s3fp,
                         s3_fancy_json_io_manager as s3fj)
from .resources import MyConnectionResource, s3_rsrce
from .assets import (raw, raw_io_managers_use, 
                    raw_resource_use, spark_transformations, 
                    graph_raw
                    )
# from .sensors import stocks_sensor

all_assets = load_assets_from_modules([raw, raw_io_managers_use, raw_resource_use, spark_transformations, graph_raw])
# all_sensors = [stocks_sensor]

defs = Definitions(
    assets= all_assets,
    # sensors= all_sensors,
    resources= {
        "my_conn": MyConnectionResource(access_token=EnvVar("FINNHUBAPIKEY")),
        "s3": s3_rsrce,
        "s3_io_manager": S3PickleIOManager(
            s3_resource=s3_rsrce, s3_bucket="dagster-api", s3_prefix="source"
        ),
        "fs_io_manager": FilesystemIOManager(),
        "s3_prqt_io_manager": s3p.S3PandasParquetIOManager(
            s3_resource=s3_rsrce, s3_bucket="dagster-api", s3_prefix="source"
        ),
        "s3_json_io_manager": s3j.S3JSONIOManager(
            s3_resource=s3_rsrce, s3_bucket="dagster-api", s3_prefix="raw"
        ),
        "s3_fancy_prqt_io_manager": s3fp.S3PandasParquetIOManager(
            s3_resource=s3_rsrce, s3_bucket="dagster-api", s3_prefix="source"
        ),
        "s3_fancy_json_io_manager": s3fj.S3JSONIOManager(
            s3_resource=s3_rsrce, s3_bucket="dagster-api", s3_prefix="source"
        ),
    },
)
