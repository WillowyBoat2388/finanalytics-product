from dagster import (Definitions, 
                    load_assets_from_modules, 
                    EnvVar)
from dagster_deltalake import LocalConfig, S3Config, DeltaLakePyarrowIOManager
from dagster_deltalake_pandas import DeltaLakePandasIOManager
from dagster_snowflake_pyspark import SnowflakePySparkIOManager
from .iomanagers import (raw_s3_json_io_manager as s3j, 
                         staging_s3_parquet_io_manager as s3p,)
from .resources import MyConnectionResource, s3_rsrce, MyPysparkResource
from .assets import (spark_transformations, 
                    graph_raw, warehouse_tables
                    )
# from .sensors import stocks_sensor

all_assets = load_assets_from_modules([spark_transformations, graph_raw, warehouse_tables])
# all_sensors = [stocks_sensor]

# config = S3Config(endpoint=EnvVar("AWS_ENDPOINT"), allow_unsafe_rename=True)

defs = Definitions(
    assets= all_assets,
    # sensors= all_sensors,
    resources= {
        "my_conn": MyConnectionResource(access_token=EnvVar("FINNHUBAPIKEY")),
        "s3": s3_rsrce,
        "pyspark": MyPysparkResource(),
        "s3_prqt_io_manager": s3p.S3PandasParquetIOManager(
            pyspark=MyPysparkResource(), s3_resource=s3_rsrce, s3_bucket="dagster-api", s3_prefix="staging"
        ),
        "s3_json_io_manager": s3j.S3JSONIOManager(
            s3_resource=s3_rsrce, s3_bucket="dagster-api", s3_prefix="raw"
        ),
        "delta_lake_arrow_io_manager": DeltaLakePyarrowIOManager(
            root_uri="dagster-api",  # required
            storage_options=S3Config(access_key_id=EnvVar("AWS_ACCESS_KEY_ID"), 
                                     secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"), region=EnvVar("AWS_REGION"), 
                                     endpoint=EnvVar("AWS_ENDPOINT"),
                                     bucket=EnvVar("AWS_BUCKET"),
                                     allow_unsafe_rename=True),  # required
            schema="core",  # optional, defaults to "public"
        ),
        "delta_lake_io_manager": DeltaLakePandasIOManager(
            root_uri="dagster-api",  # required
            storage_options=S3Config(access_key_id=EnvVar("AWS_ACCESS_KEY_ID"), 
                                     secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"), region=EnvVar("AWS_REGION"), 
                                     endpoint=EnvVar("AWS_ENDPOINT"),
                                     bucket=EnvVar("AWS_BUCKET"),
                                     allow_unsafe_rename=True), #LocalConfig(),  # required
            schema="core",  # optional, defaults to "public"
        ),
        "warehouse_io_manager": SnowflakePySparkIOManager(
            account="abc1234.us-east-1",  # required
            user=EnvVar("SNOWFLAKE_USER"),  # required
            password=EnvVar("SNOWFLAKE_PASSWORD"),  # password or private key required
            database="FLOWERS",  # required
            role="writer",  # optional, defaults to the default role for the account
            warehouse="PLANTS",  # optional, defaults to default warehouse for the account
            schema="IRIS",  # optional, defaults to PUBLIC
        )
    },
)