from dotenv import load_dotenv
import os
from sqlalchemy import Table, Column, Integer, String, MetaData, Float
from connectors.fuel_api import FuelAPIClient
from connectors.postgres_client import PostgreSqlClient
from assets.fuel_extract import extract, load, transform
from assets.pipeline_logging import PipelineLogging


def load_config():
    load_dotenv()
    return {
        "API": os.getenv('APIKEY'),
        "APISECRET": os.getenv('APISECRET'),
        "AUTHORIZATIONHEADER": os.getenv('AUTHORIZATIONHEADER'),
        "DB_USERNAME": os.environ.get("DB_USERNAME"),
        "DB_PASSWORD": os.environ.get("DB_PASSWORD"),
        "SERVER_NAME": os.environ.get("DB_SERVER_NAME"),
        "DATABASE_NAME": os.environ.get("DB_DATABASE_NAME"),
        "PORT": os.environ.get("PORT"),
    }


def create_table_metadata(table_name, columns):
    metadata = MetaData()
    return Table(table_name, metadata, *columns)


def main():
    config = load_config()

    # Configure logging
    logger = PipelineLogging('fuel_pipeline', '../logs')

    try:
        # Initialize the Fuel API client
        testAPI = FuelAPIClient(
            config["API"], config["APISECRET"], config["AUTHORIZATIONHEADER"]
        )

        # Extract data from the API
        data_station, data_fuel = extract(testAPI)

        # Transform the data into dataframes
        df_station = transform(data_station, table="station")
        df_fuel = transform(data_fuel, table="fuel")

        # Initialize the PostgreSQL client
        postgresql_client = PostgreSqlClient(
            server_name=config["SERVER_NAME"],
            database_name=config["DATABASE_NAME"],
            username=config["DB_USERNAME"],
            password=config["DB_PASSWORD"],
            port=config["PORT"]
        )

        # Create table metadata and load data
        table_station = create_table_metadata("station", [
            Column("station_code", Integer, primary_key=True),
            Column("lat", Float, primary_key=True),
            Column("lon", Float, primary_key=True),
            Column("name", String),
            Column("brand", String),
            Column("address", String),
            Column("state", String)
        ])

        # Load station data
        load(df_exchange=df_station,
             postgresql_client=postgresql_client, table=table_station)

        # Load fuel data
        table_fuel = create_table_metadata("fuel_prices", [
            Column("station_code", Integer, primary_key=True),
            Column("last_updated", String, primary_key=True),
            Column("fuel_type", String, primary_key=True),
            Column("price", Float),
            Column("state", String)
        ])
        load(df_exchange=df_fuel,
             postgresql_client=postgresql_client, table=table_fuel)

        # Log successful execution
        logger.info("Data loading completed successfully")

    except Exception as e:
        # Log the error and handle it gracefully
        logger.error(f"An error occurred: {e}")


if __name__ == '__main__':
    main()
