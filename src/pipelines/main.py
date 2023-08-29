import pandas as pd
from dotenv import load_dotenv
import os 
from src.connectors.nsw_api import NswApiClient

if __name__ == "__main__":
    load_dotenv()
    AUTH = os.environ.get("AUTH")

    api_client = NswApiClient(auth=AUTH, tranid=1)

    print(api_client.extract_nsw_fuel_data())
    
