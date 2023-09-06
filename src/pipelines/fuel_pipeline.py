from dotenv import load_dotenv
import os
from sqlalchemy import Table, Column, Integer, String, MetaData, Float
from connectors.fuel_api import FuelAPIClient
from connectors.postgres_client import PostgreSqlClient
from assets.fuel_extract import extract, load, transform


def load_config():
    load_dotenv()
    return {
        "API": os.getenv('APIKEY'),
        "APISECRET": os.getenv('APISECRET'),
        "AUTHORIZATIONHEADER": os.getenv('AUTHORIZATIONHEADER'),
        "DB_USERNAME": os.getenv("DB_USERNAME"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD"),
        "SERVER_NAME": os.getenv("DB_SERVER_NAME"),
        "DATABASE_NAME": os.getenv("DB_DATABASE_NAME"),
        "PORT": os.getenv("PORT"),
    }


def create_database_tables():
    station_metadata = MetaData()
    table_station = Table(
        "station", station_metadata,
        Column("station_code", Integer, primary_key=True),
        Column("lat", Float, primary_key=True),
        Column("lon", Float, primary_key=True),
        Column("name", String),
        Column("brand", String),
        Column("address", String),
        Column("state", String)
    )
    fuel_metadata = MetaData()
    table_fuel = Table(
        "fuel_prices", fuel_metadata,
        Column("station_code", Integer, primary_key=True),
        Column("last_updated", String, primary_key=True),
        Column("fuel_type", String, primary_key=True),
        Column("price", Float),
        Column("state", String)
    )
    return table_station, table_fuel


def main():
    config = load_config()
    testAPI = FuelAPIClient(
        config["API"], config["APISECRET"], config["AUTHORIZATIONHEADER"])
    data_station, data_fuel = extract(testAPI)
    df_station = transform(data_station, table="station")
    df_fuel = transform(data_fuel, table="fuel")

    postgresql_client = PostgreSqlClient(
        server_name=config["SERVER_NAME"],
        database_name=config["DATABASE_NAME"],
        username=config["DB_USERNAME"],
        password=config["DB_PASSWORD"],
        port=config["PORT"]
    )

    table_station, table_fuel = create_database_tables()
    fuel_metadata = MetaData()
    station_metadata = MetaData()
    try:
        # Run the 'load' function once immediately
        load(df_exchange=df_fuel, postgresql_client=postgresql_client,
             table=table_fuel, metadata=fuel_metadata)
        load(df_exchange=df_station, postgresql_client=postgresql_client,
             table=table_station, metadata=station_metadata)
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == '__main__':
    main()
