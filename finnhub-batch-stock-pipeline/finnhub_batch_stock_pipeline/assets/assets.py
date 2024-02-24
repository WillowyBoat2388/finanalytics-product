from dagster import asset
import requests
import json
import pandas as pd
from . import constants
from ..resources import FinnhubClientResource, MyConnectionResource


@asset()
def scrape_data(api_key:FinnhubClientResource, my_conn:MyConnectionResource):
    """
        Template for loading data from API
    """
    
    # api_key = get_secret_value('API_Key')
    
    # Create a dictionary with the API key as a parameter
    headers = {'X-Finnhub-Token' : api_key.api_token()}

    endpoint = "stock/metric?symbol=AAPL&metric=all"

    

    #Finnhub dynamic API URL for extracting data
    url = 'https://finnhub.io/api/v1/stock/metric?symbol=AAPL&metric=all'

    respond = my_conn.request(endpoint)

    response = requests.get(url,headers=headers)

    print(respond)
    print(response)

    data = response.json()
    data_body = respond.json()

    df_2 = pd.DataFrame([data_body])
    df = pd.DataFrame([data])

    df.to_csv(constants.FILE_PATH, index=False)

    df_2.to_csv(constants.FILE_PATH.format("_2"), index=False)

