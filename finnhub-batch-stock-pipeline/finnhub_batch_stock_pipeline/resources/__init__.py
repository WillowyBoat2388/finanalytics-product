import requests
import os
from requests import Response
from dagster_aws.s3 import S3Resource
from dagster import ConfigurableResource, EnvVar
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from delta import *



class MyPysparkResource():
    def __init__(self) -> None:
        builder = SparkSession.builder.appName("Dagster_FinnHub") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") \
            .config("spark.hadoop.fs.s3a.endpoint.region", 'eu-west-1') \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")   
            # .config('spark.hadoop.fs.s3a.access.key', EnvVar("AWS_ACCESS_KEY").get_value()).config('spark.hadoop.fs.s3a.secret.key', os.getenv("AWS_SECRET_KEY"))

        self.spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"]).getOrCreate()
        # self.conf = sparkConf
        #     .setMaster(EnvVar("SPARK_MASTER"))
        self.sc = self.spark.sparkContext
        # self.sc.setLogLevel("ERROR")


    def load_s3(self, path) -> None:
        return self.spark.read.format("delta").load(path)

class FinnhubClientResource(ConfigurableResource):
    access_token: str

    def api_token(self):
        return self.access_token

class MyConnectionResource(ConfigurableResource):
    access_token: str

    def request(self, endpoint: str) -> Response:

        #Finnhub dynamic API URL for extracting data

        response = requests.get(
            f"https://finnhub.io/api/v1/{endpoint}",
            headers={"X-Finnhub-Token" : self.access_token},
        )
        
        if response.status_code != 200:
            return response.raise_for_status()
        else:
            data = response.json()

        return data
    
s3_rsrce = S3Resource(use_unsigned_session=True,
                    region_name="eu-west-1",
                    endpoint_url="http://127.0.0.1:9000", 
                    use_ssl= False, 
                    aws_access_key_id = EnvVar("access_id"),
    aws_secret_access_key= EnvVar("access_key"),)


