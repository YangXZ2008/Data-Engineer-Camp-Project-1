import pandas as pd
from dotenv import load_dotenv
import os 
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable 
from src.connectors.nsw_api import NswApiClient
from src.connectors.postgresql import PostgreSqlClient
from src.assests.nsw_api import extract, transform, load



if __name__ == "__main__":
    load_dotenv()
    AUTH = os.environ.get("AUTH")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")

    nsw_api_client = NswApiClient(auth=AUTH, tranid=1)
    df_prices, df_stations = extract(api_client = nsw_api_client)

    df_prices_final, df_stations_final = transform(prices_df=df_prices, stations_df=df_stations )
