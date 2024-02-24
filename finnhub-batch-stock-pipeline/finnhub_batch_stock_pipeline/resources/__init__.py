from dagster import ConfigurableResource, StringSource, resource, EnvVar
import requests
from requests import Response

class FinnhubClientResource(ConfigurableResource):
    access_token: str

    def api_token(self):
        return self.access_token

class MyConnectionResource(ConfigurableResource):
    access_token: str

    def request(self, endpoint: str) -> Response:

        #Finnhub dynamic API URL for extracting data

        return requests.get(
            f"https://finnhub.io/api/v1/{endpoint}",
            headers={"X-Finnhub-Token" : self.access_token},
        )