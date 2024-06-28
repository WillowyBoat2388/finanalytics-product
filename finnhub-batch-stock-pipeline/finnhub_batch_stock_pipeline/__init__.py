from dagster import (Definitions, 
                    load_assets_from_modules, 
                    EnvVar,
                    AutoMaterializePolicy,
                    multiprocess_executor,
                    in_process_executor)
from dagster_duckdb_pyspark import DuckDBPySparkIOManager
from dagster_k8s import k8s_job_executor
from dagster_pyspark import LazyPySparkResource
from dagster_deltalake import S3Config, DeltaLakePyarrowIOManager
from dagster_deltalake.config import ClientConfig
from dagster_snowflake_pyspark import SnowflakePySparkIOManager
from .iomanagers import (raw_s3_json_io_manager as s3j, 
                         staging_s3_parquet_io_manager as s3p,)
from .resources import MyConnectionResource, s3_rsrce#, MyPysparkResource
from .assets import (spark_transformations, 
                    graph_raw, fact_tables,
                    )
from .jobs import stock_retrieval_job, lake_update_job, warehouse_update_job, spark_transformation_job
from .schedules import stocks_update_schedule
import os
import uuid


# dictionary to configure job executors and their specific required configs
execution = {'inprocess': in_process_executor,
            'multiprocess': multiprocess_executor.configured({ "max_concurrent": 8}),
            'k8s': k8s_job_executor.configured({
                "job_image": "spark:python3-java17",
                "image_pull_policy": "IfNotPresent",
                "max_concurrent": 4, "step_k8s_config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "1000m", "memory": "5120Mi"},
                    "limits": {"cpu": "2000m", "memory": "10240Mi"},
                }
            }
        }
            })
            }
# variable which contains all jobs for code location definition
all_assets = load_assets_from_modules([fact_tables, spark_transformations, graph_raw], auto_materialize_policy=AutoMaterializePolicy.eager())

# variable which contains all jobs for code location definition
all_jobs = [stock_retrieval_job, lake_update_job, warehouse_update_job, spark_transformation_job]
# variable which contains all schedules for code location definition
all_schedules = [stocks_update_schedule]
# config for delta lake arrow iomanager to use S3
config = S3Config(access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), 
                                    secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"), region=os.getenv("AWS_REGION"), 
                                    endpoint=os.getenv("AWS_ENDPOINT"),
                                    bucket=os.getenv("AWS_BUCKET"),
                                    allow_unsafe_rename=True)
# config for deltalake arrow iomanager client
client_config = ClientConfig(allow_http=True)

jars = [
    "jars/delta-spark_2.12-3.1.0.jar",
    "jars/delta-core_2.12-2.4.0.jar",
    "jars/delta-storage-3.1.0.jar",
    "jars/hadoop-aws-3.3.4.jar",
    "jars/hadoop-common-3.3.4.jar",
    "jars/aws-java-sdk-bundle-1.12.262.jar"
]

# Join the paths with a comma separator
jars_paths = ",".join(jars)

pyspark_config = {
                "spark.jars": jars_paths,
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.sql.execution.arrow.pyspark.enabled": "true",
                "spark.hadoop.fs.s3a.endpoint": os.getenv("AWS_ENDPOINT"),
                "spark.hadoop.fs.s3a.endpoint.region": os.getenv('AWS_REGION'),
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                # "spark.driver.memory": "4g",
                "spark.dynamicAllocation.enabled": "true",
                "spark.shuffle.service.enabled": "true",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            }
# config for pyspark resource using inprocess executor
pyspark_config_inprocess = {
                # "spark.jars": jars_paths,
                "spark.jars.packages": "io.delta:delta-storage:3.1.0,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.sql.execution.arrow.pyspark.enabled": "true",
                "spark.hadoop.fs.s3a.endpoint": os.getenv("AWS_ENDPOINT"),
                "spark.hadoop.fs.s3a.endpoint.region": os.getenv('AWS_REGION'),
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.driver.memory": "4g",
                "spark.dynamicAllocation.enabled": "true",
                "spark.shuffle.service.enabled": "true",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            }
# Dictionary for determining config for pyspark resource using environment variable
spark_config = {
    "inprocess": pyspark_config_inprocess,
    "multiprocess_in": pyspark_config_inprocess,
    "multiprocess": pyspark_config,
    "k8s": pyspark_config
}
deployment_resources = {
       "prod":  {
        "my_conn": MyConnectionResource(access_token=EnvVar("FINNHUBAPIKEY")),
        "s3": s3_rsrce,
        "pyspark": LazyPySparkResource(
            spark_config=spark_config[os.getenv("EXECUTOR")]
        ),
        "s3_prqt_io_manager": s3p.S3PandasParquetIOManager(
            pyspark=LazyPySparkResource(spark_config=spark_config[os.getenv("EXECUTOR")]), 
            s3_resource=s3_rsrce, # required
            s3_bucket="dagster-api", # required
            s3_prefix="staging" # required
        ),
        "s3_json_io_manager": s3j.S3JSONIOManager(
            s3_resource=s3_rsrce, # required
            s3_bucket="dagster-api", # required
            s3_prefix="raw" # required
        ),
        "delta_lake_arrow_io_manager": DeltaLakePyarrowIOManager(
            root_uri=EnvVar("S3_URI"),  # required
            storage_options=config,  # required
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
    "staging": {
        "my_conn": MyConnectionResource(access_token=EnvVar("FINNHUBAPIKEY")),
        "s3": s3_rsrce,
        "pyspark": LazyPySparkResource(
            spark_config=spark_config[os.getenv("EXECUTOR")]
        ),
        "s3_prqt_io_manager": s3p.S3PandasParquetIOManager(
            pyspark=LazyPySparkResource(spark_config=spark_config[os.getenv("EXECUTOR")]), 
            s3_resource=s3_rsrce, # required
            s3_bucket="dagster-api", # required
            s3_prefix="staging" # required
        ),
        "s3_json_io_manager": s3j.S3JSONIOManager(
            s3_resource=s3_rsrce, # required
            s3_bucket="dagster-api", # required
            s3_prefix="raw" # required
        ),
        "delta_lake_arrow_io_manager": DeltaLakePyarrowIOManager(
            root_uri="s3://dagster-api",  # required
            storage_options=config,  # required
            client_options=client_config,
            schema="core",  # optional, defaults to "public"
        ),
        "warehouse_io_manager": SnowflakePySparkIOManager(
            account=EnvVar("SNOWFLAKE_ACCOUNT"),  # required
            user=EnvVar("SNOWFLAKE_USER"),  # required
            password=EnvVar("SNOWFLAKE_PASSWORD"),  # password or private key required
            database=EnvVar("SNOWFLAKE_DB"),  # required
        )
    },
    "local": {
        "my_conn": MyConnectionResource(access_token=EnvVar("FINNHUBAPIKEY")),
        "s3": s3_rsrce,
        "pyspark": LazyPySparkResource(
            spark_config=spark_config[os.getenv("EXECUTOR")]
        ),
        "s3_prqt_io_manager": s3p.S3PandasParquetIOManager(
            pyspark=LazyPySparkResource(spark_config=spark_config[os.getenv("EXECUTOR")]), 
            s3_resource=s3_rsrce, # required
            s3_bucket="dagster-api", # required
            s3_prefix="staging" # required
        ),
        "s3_json_io_manager": s3j.S3JSONIOManager(
            s3_resource=s3_rsrce, # required
            s3_bucket="dagster-api", # required
            s3_prefix="raw" # required
        ),
        "delta_lake_arrow_io_manager": DeltaLakePyarrowIOManager(
            root_uri="s3://dagster-api",  # required
            storage_options=config,  # required
            client_options=client_config,
            schema="core",  # optional, defaults to "public"
        ),
        "warehouse_io_manager": DuckDBPySparkIOManager(
            database="finance_db.duckdb", 
            schema="core"
        )
    }
}
# Environment variable to determine deployment environment
deployment_name = os.getenv("DAGSTER_DEPLOYMENT", "local")

# Dagster job executor environment variable
executor = os.getenv("EXECUTOR", "k8s")

# Dagster code location definitions
defs = Definitions(
    assets= all_assets,
    resources= deployment_resources[deployment_name],
    jobs=all_jobs,
    schedules=all_schedules,
    executor=execution[executor if executor != "multiprocess_in" else "multiprocess"],
)