import requests
from pprint import pprint
import json
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime

class FuelAPIClient:

    def __init__(self, api_key, api_secret_key, authorisation):
        self.api_key = api_key 
        self.api_secret_key = api_secret_key
        self.authorisation = authorisation

        assert self.api_key != None, "API Key cannot be None"
        assert self.api_secret_key != None, "API Secret Key cannot be None"
        assert self.authorisation != None, "Authorisation cannot be None"
        self.ACCESS_TOKEN_URL = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken"
        self.access_code = None
        self.access_token_file = "src/connectors/access_token.json"
        self.fuel_api_url = "https://api.onegov.nsw.gov.au/FuelPriceCheck/v2/fuel/prices?states=NSW"

    def get_access_token(self):
        if os.path.exists(self.access_token_file):
            token_info = json.load(open(self.access_token_file))
            issued_time = datetime.fromtimestamp(int(token_info["issued_at"])/1e3)
            current_time = datetime.now()
            if (current_time - issued_time).total_seconds() < int(token_info["expires_in"]):
                # old token is still valid
                print("old token is still valid!")
                self.access_code = token_info["access_token"]
                return True
        
        query_string = {"grant_type":"client_credentials"}
        headers = {
                'content-type': "application/json",
                'authorization': self.authorisation 
                }

        response = requests.request("GET", self.ACCESS_TOKEN_URL, headers=headers, params=query_string)
        if response.status_code != 200:
            raise Exception(f"Failed to extract data from the Fuel API. Status Code: {response.status_code}. Response: {response.text}")

        self.access_code = response.json()['access_token']
        assert self.access_code != None, "Error getting access token"
        # Write the access token to a file for next use
        json_object = json.dumps(response.json(), indent=4)
        with open("src/connectors/access_token.json", "w") as outfile:
            outfile.write(json_object)

    def get_fuel_data(self):
        if self.access_code is None:
            self.get_access_token()
        headers = {
            'accept' : "application/json",
            'content-type': "application/json; charset=utf-8",
            'authorization': "Bearer " + self.access_code,
            'apikey': self.api_key,
            'transactionid': "1",
            'requesttimestamp': "29/08/2023 08:06:30 PM"
            }
        response = requests.request("GET", self.fuel_api_url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Failed to extract data from the Fuel API. Status Code: {response.status_code}. Response: {response.text}")

        data = response.json()
        # print(type(data))
        # print(data.keys())
        # df = pd.json_normalize(data)
        # df.to_csv("src/connectors/sample_fuel.csv")

        return data
        

