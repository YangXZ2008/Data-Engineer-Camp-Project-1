import json
from pprint import pprint
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime

from src.connectors.fuel_api import FuelAPIClient


def extract(fuel_client: FuelAPIClient):
    data = fuel_client.get_fuel_data()
    df_stations = pd.json_normalize(data["stations"])
    df_fuel_prices = pd.json_normalize(data["prices"])
    # save the extracted data to csv for now
    df_stations.to_csv("src/data/stations.csv")
    df_fuel_prices.to_csv("src/data/fuel_prices.csv")


def transform():
    pass


if __name__ == '__main__':

    load_dotenv()
    API = os.getenv('APIKEY')
    APISECRET = os.getenv('APISECRET')
    AUTHORIZATIONHEADER = os.getenv('AUTHORIZATIONHEADER')
    testAPI = FuelAPIClient(API, APISECRET, AUTHORIZATIONHEADER)
    # testAPI.get_access_token()
    # testAPI.get_fuel_data()
    extract(testAPI)
