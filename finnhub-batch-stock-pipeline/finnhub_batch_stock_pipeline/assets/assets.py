from dagster import asset
import requests
import json
import pandas as pd
from . import constants
from ..resources import FinnhubClientResource


@asset()
def scrape_data(api_key:FinnhubClientResource):
    """
        Asset for loading data from FinnHub API to get stock symbols
    """
    
    # Create a dictionary with the API key as a parameter
    headers = {'X-Finnhub-Token' : api_key.api_token()}

    # endpoint = "stock/metric?symbol=AAPL&metric=all"
    # respond = my_conn.request(endpoint)
    

    #Finnhub dynamic API URL for extracting data
    url = 'https://finnhub.io/api/v1/stock/metric?symbol=AAPL&metric=all'

    response = requests.get(url,headers=headers)

