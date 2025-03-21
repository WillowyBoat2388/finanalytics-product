import requests
import os
from requests import Response
from dagster_aws.s3 import S3Resource
from dagster import ConfigurableResource, EnvVar
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
# from delta import *
import pyarrow
import pyarrow.parquet as pq

class FinnhubClientResource(ConfigurableResource):
    """
    A class definition for dealing with the api key
    for the project. Takes in the api key in its instantiation
    And when the api_token method is called, it returns said
    value.
    """
    access_token: str
    
    def api_token(self):
        return self.access_token

class MyConnectionResource(ConfigurableResource):
    """
    A class definition for dealing with the api key
    for the project. Takes in the api key in its instantiation
    And when the request method is called along with the endpoint
    that data is to be retrieved from, it returns the output of
    calling requests.get on the endpoint using the token
    """
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
    
# Resource variable for dealing with S3 connection using boto3 under the hood
s3_rsrce = S3Resource(use_unsigned_session=True,
                    region_name=EnvVar("AWS_REGION"),
                    endpoint_url=EnvVar("AWS_ENDPOINT"), 
                    use_ssl= False, 
                    aws_access_key_id = EnvVar("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key= EnvVar("AWS_SECRET_ACCESS_KEY"),)


# class MyPysparkResource():
#     def __init__(self) -> None:
#         builder = SparkSession.builder.appName("Dagster_FinnHub") \
#             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#             .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
#             .config("spark.hadoop.fs.s3a.endpoint", os.getenv("AWS_ENDPOINT")) \
#             .config("spark.hadoop.fs.s3a.endpoint.region", os.getenv('AWS_REGION')) \
#             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
#             .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#             .config("spark.executor.memory", "4g") \
#             .config("spark.driver.memory", "4g") \
#             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")   
#             # .config('spark.hadoop.fs.s3a.access.key', EnvVar("AWS_ACCESS_KEY").get_value()).config('spark.hadoop.fs.s3a.secret.key', os.getenv("AWS_SECRET_KEY"))

#         self.spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"]).getOrCreate()
#         # self.conf = sparkConf
#         #     .setMaster(EnvVar("SPARK_MASTER"))
#         self.sc = self.spark.sparkContext
#         # self.sc.setLogLevel("ERROR")


#     def load_s3(self, path) -> None:
#         return self.spark.read.format("delta").load(path)
