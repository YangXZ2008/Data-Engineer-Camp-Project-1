import pandas as pd
from dotenv import load_dotenv
import os 
from sqlalchemy import Table, Column, Integer, String, MetaData, Float
from etl_project.connectors.nsw_api import NswApiClient
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.assests.nsw_api import extract, transform_2_df, load



if __name__ == "__main__":
    load_dotenv()
    AUTH = f"Basic {os.environ.get('AUTH')}"
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")

    nsw_api_client = NswApiClient(auth=AUTH, tranid=1)
    
    df_prices, df_stations = extract(api_client = nsw_api_client.extract_nsw_fuel_data())

    df_prices_final, df_stations_final = transform_2_df(prices_df=df_prices, stations_df=df_stations )


    #DB Connection
    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT
    )
    # create ORM model 
    meta = MetaData()
    fuel_stations_tbl = Table(
        "nsw_stations", meta, 
        Column("name", String),
        Column("station_code", Integer),
        Column("address", String),
        Column("lat", Float),
        Column("long", Float)
    )

    fuel_prices_tbl = Table(
        "nsw_fuel_price", meta,
        Column('id', String, primary_key=True),
        Column("station_code", Integer),
        Column("fuel_type", String),
        Column("price", Float),
        Column("last_updated", String, primary_key=True)
    )

    load(df=df_stations_final, pg_client=postgresql_client, table=fuel_stations_tbl, metadata=meta, loadtype='normal')

    load(df=df_prices_final, pg_client=postgresql_client, table=fuel_prices_tbl, metadata=meta, loadtype='chunk')
    print("Successful")