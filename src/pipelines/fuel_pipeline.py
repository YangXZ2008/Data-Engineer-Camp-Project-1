import json
from pprint import pprint
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime
from sqlalchemy import Table, Column, Integer, String, MetaData, Float
from src.connectors.fuel_api import FuelAPIClient
from src.connectors.postgres_client import PostgreSqlClient
from src.assets.fuel_extract import extract, load, transform



if __name__ == '__main__':

    load_dotenv()
    API = os.getenv('APIKEY')
    APISECRET = os.getenv('APISECRET')
    AUTHORIZATIONHEADER = os.getenv('AUTHORIZATIONHEADER')
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("DB_SERVER_NAME")
    DATABASE_NAME = os.environ.get("DB_DATABASE_NAME")
    PORT = os.environ.get("PORT") 
    
    testAPI = FuelAPIClient(API, APISECRET, AUTHORIZATIONHEADER)
    # testAPI.get_access_token()
    # testAPI.get_fuel_data()
    data_station, data_fuel = extract(testAPI)

    df_station = transform(data_station, table="station")
    df_fuel = transform(data_fuel, table="fuel")

    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT
    )
    
    metadata_station = MetaData()
    table_station = Table(
        "station", metadata_station, 
        Column("station_code", Integer, primary_key=True),
        Column("lat", Float, primary_key=True),
        Column("lon", Float, primary_key=True),
        Column("name", String),
        Column("brand", String),
        Column("address", String),
        Column("state", String)
    )
    load(df_exchange=df_station, postgresql_client=postgresql_client, table=table_station, metadata=metadata_station)

    metadata_fuel = MetaData()
    table_fuel = Table(
        "fuel_prices", metadata_fuel,
        Column("station_code", Integer, primary_key=True),
        Column("last_updated", String, primary_key=True),
        Column("fuel_type", String, primary_key=True),
        Column("price", Float),
        Column("state", String)
    )

    if df_fuel.shape[0] > 5000:
        load(df_exchange=df_fuel[:5000], postgresql_client=postgresql_client, table=table_fuel, metadata=metadata_fuel)
        load(df_exchange=df_fuel[5000:], postgresql_client=postgresql_client, table=table_fuel, metadata=metadata_fuel)
    else:
        load(df_exchange=df_fuel, postgresql_client=postgresql_client, table=table_fuel, metadata=metadata_fuel)