
from src.connectors.fuel_api import FuelAPIClient
from dotenv import load_dotenv
import os
import pytest

@pytest.fixture
def setup():
    load_dotenv()

def test_get_access_token(setup):
    # Set up the FuelAPIClient instance with dummy credentials
    client = FuelAPIClient("dummy_api_key", "dummy_api_secret_key", "dummy_authorisation")
    
    # Call the get_access_token method
    client.get_access_token()

    # Check that access code is not None after getting the token
    assert client.access_code is not None

def test_get_fuel_data():
    # Set up the FuelAPIClient instance with dummy credentials
    client = FuelAPIClient("dummy_api_key", "dummy_api_secret_key", "dummy_authorisation")
    
    # Call the get_fuel_data method
    fuel_data = client.get_fuel_data()

    # Check that fuel data is not None
    assert fuel_data is not None

