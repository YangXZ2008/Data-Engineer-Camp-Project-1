import json
from pprint import pprint
import pandas as pd

if __name__ == '__main__':
    with open("src/data/sample_response.json", "r") as read_file:
        data = json.load(read_file) 
    
    pprint(type(data))

    df_stations = pd.json_normalize(data["stations"])
    df_fuel_prices = pd.json_normalize(data["prices"])

    df_stations.to_csv("src/data/stations.csv")
    df_fuel_prices.to_csv("src/data/fuel_prices.csv")