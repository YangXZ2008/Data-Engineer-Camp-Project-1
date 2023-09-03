import requests
from datetime import datetime

class NswApiClient:
    def __init__(self, auth:str, tranid:int) -> None:
        self.token_base_url = 'https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken?grant_type=client_credentials'
        self.fuel_base_url = "https://api.onegov.nsw.gov.au/FuelPriceCheck/v1/fuel/prices"
        self.auth = auth
        self.tranid = tranid
        self.format_data = "%d/%m/%y %I:%M:%S %p"
        self.timestamp = datetime.now().strftime(self.format_data)

    def get_nsw_auth_api(self) -> dict:
        auth_items = {}
        headers = {
            "Authorization": self.auth
        }
        response = requests.get(self.token_base_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            auth_items['client_id'] = data.get('client_id')
            auth_items['access_token'] =f"Bearer {data.get('access_token')}"
        else:
            raise Exception("Authorization is missing.")
        return auth_items

    def extract_nsw_fuel_data(self) -> dict:
        #get authen tication
        authentication = self.get_nsw_auth_api()

        headers = {
            "Authorization": authentication["access_token"],
            "Content-Type": "application/json; charset=utf-8",
            "apikey": authentication["client_id"],
            "transactionid": f"{self.tranid}",
            "requesttimestamp": self.timestamp
        }

        response = requests.get(self.fuel_base_url, headers=headers)
        if response.status_code == 200 and response.json().get("prices") is not None and response.json().get("stations") is not None:
            return response.json() #get data
        else:
            raise Exception(f"Failed to extract data from NSW API. Status Code: {response.status_code}. Response: {response.text}")
