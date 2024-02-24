from dagster import Definitions, load_assets_from_modules, EnvVar
from .resources import FinnhubClientResource, MyConnectionResource
from .assets import assets

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "api_key": FinnhubClientResource(access_token=EnvVar("FINNHUBAPIKEY")),
        "my_conn": MyConnectionResource(access_token=EnvVar("FINNHUBAPIKEY")),
    },
)
