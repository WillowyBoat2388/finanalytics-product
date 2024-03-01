import requests
from requests import Response
from dagster_aws.s3 import S3Resource
from dagster import ConfigurableResource, EnvVar



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


