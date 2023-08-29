import requests
from requests.auth import AuthBase


class TokenAuth(AuthBase):
    def __init__(self, token, auth_scheme='Bearer'):
        self.token = token
        self.auth_scheme = auth_scheme

    def __call__(self, request):
        request.headers['authorization'] = f'{self.auth_scheme} {self.token}'
        request.headers["grant_type"] = "grant_type=client_credentials"
        request.headers["content-type"] = "application/json"
        return request



if __name__ == '__main__':
    import requests
    from pprint import pprint
    import pandas as pd

    # url = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken"

    # querystring = {"grant_type":"client_credentials"}

    # headers = {
    #     'content-type': "application/json",
    #     'authorization': "Basic c0YwWFdPR3dPMzNPSkxqYUU2U3pIVE4zNWVvbU83UnM6RkxGZ3ZONlhTWGRsWlZ3Tw=="
    #     }

    # response = requests.request("GET", url, headers=headers, params=querystring)

    # print(type(response.json()))
    # credentials = response.json()
    # pprint(credentials)

    url2 = "https://api.onegov.nsw.gov.au/FuelPriceCheck/v2/fuel/prices?states=NSW"

    payload = "{\"fueltype\":\"\",\"brand\":[],\"namedlocation\":\"\",\"referencepoint\":{\"latitude\":\"\",\"longitude\":\"\"},\"sortby\":\"\",\"sortascending\":\"\"}"
    headers2 = {
        'accept' : "application/json",
        'content-type': "application/json; charset=utf-8",
        'authorization': "Bearer " + "Ikb4ZnNwoBRs4GeQTQfuQDDf7t0I",
        'apikey': "sF0XWOGwO33OJLjaE6SzHTN35eomO7Rs",
        'transactionid': "1",
        'requesttimestamp': "27/08/2023 03:06:30 PM"
        }

    print("="*100)
    pprint(headers2)
    print("="*100)
    response2 = requests.request("GET", url2, headers=headers2)

    data = response2.json()

    print(type(data))
    print(data.keys())

    pprint(data["prices"])

    #df = pd.json_normalize(data)
    #df.to_csv("sample_fuel.csv")
    # print(response2)
    # print(response2.text)
