from dagster import (Definitions, 
                    load_assets_from_modules, 
                    EnvVar)
from dagster_duckdb_pyspark import DuckDBPySparkIOManager
from dagster_pyspark import LazyPySparkResource
from dagster_deltalake import S3Config, DeltaLakePyarrowIOManager
from dagster_deltalake.config import ClientConfig
from dagster_deltalake_pandas import DeltaLakePandasIOManager
from dagster_snowflake_pyspark import SnowflakePySparkIOManager
from .iomanagers import (raw_s3_json_io_manager as s3j, 
                         staging_s3_parquet_io_manager as s3p,)
from .resources import MyConnectionResource, s3_rsrce#, MyPysparkResource
from .assets import (spark_transformations, 
                    graph_raw, fact_tables,
                    )
import os
# from .sensors import stocks_sensor

all_assets = load_assets_from_modules([fact_tables, spark_transformations, graph_raw])
# all_sensors = [stocks_sensor]

# config = S3Config(endpoint=EnvVar("AWS_ENDPOINT"), allow_unsafe_rename=True)
config = S3Config(access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), 
                                    secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"), region=os.getenv("AWS_REGION"), 
                                    endpoint=os.getenv("AWS_ENDPOINT"),
                                    bucket=os.getenv("AWS_BUCKET"),
                                    allow_unsafe_rename=True)
client_config = ClientConfig(allow_http=True)

pyspark_config = {
                "spark.jars.packages": "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.sql.execution.arrow.pyspark.enabled": "true",
                "spark.hadoop.fs.s3a.endpoint": os.getenv("AWS_ENDPOINT"),
                "spark.hadoop.fs.s3a.endpoint.region": os.getenv('AWS_REGION'),
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.executor.memory": "2g",
                "spark.dynamicAllocation.enabled": "true",
                "spark.shuffle.service.enabled": "true",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            }

deployment_resources = {
       "prod":  {
        "my_conn": MyConnectionResource(access_token=EnvVar("FINNHUBAPIKEY")),
        "s3": s3_rsrce,
        # "pyspark": MyPysparkResource(),
        "pyspark": LazyPySparkResource(
            spark_config=pyspark_config
        ),
        "s3_prqt_io_manager": s3p.S3PandasParquetIOManager(
            pyspark=LazyPySparkResource(spark_config=pyspark_config), s3_resource=s3_rsrce, s3_bucket="dagster-api", s3_prefix="staging"
        ),
        "s3_json_io_manager": s3j.S3JSONIOManager(
            s3_resource=s3_rsrce, s3_bucket="dagster-api", s3_prefix="raw"
        ),
        "delta_lake_arrow_io_manager": DeltaLakePyarrowIOManager(
            root_uri=EnvVar("S3_URI"),  # required
            storage_options=config,  # required
            client_options=client_config,
            schema="core",  # optional, defaults to "public"
        ),
        "delta_lake_io_manager": DeltaLakePandasIOManager(
            root_uri=EnvVar("S3_URI"),  # required
            storage_options=config, #LocalConfig(),  # required
            client_options=client_config,
            schema="core",  # optional, defaults to "public"
        ),
        "warehouse_io_manager": SnowflakePySparkIOManager(
            account="abc1234.us-east-1",  # required
            user=EnvVar("SNOWFLAKE_USER"),  # required
            password=EnvVar("SNOWFLAKE_PASSWORD"),  # password or private key required
            database="FLOWERS",  # required
            role="writer",  # optional, defaults to the default role for the account
            warehouse="PLANTS",  # optional, defaults to default warehouse for the account
            schema="core",  # optional, defaults to PUBLIC
        )
    },
    "local": {
        "my_conn": MyConnectionResource(access_token=EnvVar("FINNHUBAPIKEY")),
        "s3": s3_rsrce,
        # "pyspark": MyPysparkResource(),
        "pyspark": LazyPySparkResource(
            spark_config=pyspark_config
        ),
        "s3_prqt_io_manager": s3p.S3PandasParquetIOManager(
            pyspark=LazyPySparkResource(spark_config=pyspark_config), s3_resource=s3_rsrce, s3_bucket="dagster-api", s3_prefix="staging"
        ),
        "s3_json_io_manager": s3j.S3JSONIOManager(
            s3_resource=s3_rsrce, s3_bucket="dagster-api", s3_prefix="raw"
        ),
        "delta_lake_arrow_io_manager": DeltaLakePyarrowIOManager(
            root_uri="s3://dagster-api",  # required
            storage_options=config,  # required
            client_options=client_config,
            schema="core",  # optional, defaults to "public"
        ),
        "delta_lake_io_manager": DeltaLakePandasIOManager(
            root_uri=EnvVar("S3_URI"),  # required
            storage_options=config,  # required
            client_options=client_config,
            schema="core",  # optional, defaults to "public"
        ),
        "warehouse_io_manager": DuckDBPySparkIOManager(
            database="finance_db.duckdb", schema="core"
        )
    }
}
deployment_name = os.getenv("DAGSTER_DEPLOYMENT", "local")


defs = Definitions(
    assets= all_assets,
    # sensors= all_sensors,
    resources= deployment_resources[deployment_name]
)