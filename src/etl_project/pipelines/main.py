import pandas as pd
from dotenv import load_dotenv
import os 
from sqlalchemy import Table, Column, Integer, String, MetaData, Float
from etl_project.connectors.nsw_api import NswApiClient
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.assets.nsw_api import extract, transform_2_df, load
from etl_project.assets.sql_transform import SqlTransform
from etl_project.assets.pipeline_logging import PipelineLogging
from etl_project.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
from jinja2 import Environment, FileSystemLoader
from pathlib import Path
import schedule
import time
import yaml

def run_pipeline(pipeline_config: dict, postgresql_logging_client: PostgreSqlClient):
    metadata_logging = MetaDataLogging(pipeline_name=pipeline_config.get("name"), postgresql_client=postgresql_logging_client, config=pipeline_config.get("config"))
    pipeline_logging = PipelineLogging(pipeline_name=pipeline_config.get("name"), log_folder_path=pipeline_config.get("config").get("log_folder_path"))

    AUTH = f"Basic {os.environ.get('AUTH')}"
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")

    try:
        metadata_logging.log() # start run
        pipeline_logging.logger.info("Creating DB client")
        #DB Connection
        postgresql_client = PostgreSqlClient(
            server_name=SERVER_NAME,
            database_name=DATABASE_NAME,
            username=DB_USERNAME,
            password=DB_PASSWORD,
            port=PORT
        )

        pipeline_logging.logger.info("Requesting data from NSW")
        nsw_api_client = NswApiClient(auth=AUTH, tranid=1)
        
        pipeline_logging.logger.info("Extracting NSW data")
        df_prices, df_stations = extract(api_client = nsw_api_client.extract_nsw_fuel_data())

        pipeline_logging.logger.info("Transforming NSW data")
        df_prices_final, df_stations_final = transform_2_df(prices_df=df_prices, stations_df=df_stations )

        pipeline_logging.logger.info("Creating tables in postgres")
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
        pipeline_logging.logger.info("Loading data into tables")
        load(df=df_stations_final, pg_client=postgresql_client, table=fuel_stations_tbl, metadata=meta, loadtype='insert')

        load(df=df_prices_final, pg_client=postgresql_client, table=fuel_prices_tbl, metadata=meta, loadtype='chunk')

        pipeline_logging.logger.info("Reading SQL script")
        transform_template_environment = Environment(loader=FileSystemLoader(pipeline_config.get("config").get("transform_template_path")))

        pipeline_logging.logger.info("Performing SQL transformation")
        #transform 
        analyse_fuel_price = SqlTransform(table_name="fuel_price_insight", postgresql_client=postgresql_client, environment=transform_template_environment)
        analyse_fuel_price.create_table_as()

        pipeline_logging.logger.info("Pipeline complete")
        metadata_logging.log(status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logging.get_logs()) 
        pipeline_logging.logger.handlers.clear()
    except BaseException as e:
        pipeline_logging.logger.error(f"Pipeline failed with exception {e}")
        metadata_logging.log(status=MetaDataLoggingStatus.RUN_FAILURE, logs=pipeline_logging.get_logs()) 
        pipeline_logging.logger.handlers.clear()

if __name__ == "__main__":
    load_dotenv()
    LOGGING_SERVER_NAME = os.environ.get("SERVER_NAME")
    LOGGING_DATABASE_NAME = os.environ.get("DATABASE_NAME")
    LOGGING_USERNAME = os.environ.get("DB_USERNAME")
    LOGGING_PASSWORD = os.environ.get("DB_PASSWORD")
    LOGGING_PORT = os.environ.get("PORT")

    postgresql_logging_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DATABASE_NAME,
        username=LOGGING_USERNAME,
        password=LOGGING_PASSWORD,
        port=LOGGING_PORT
    )

    #get config var
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
    else:
        raise Exception(f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name.")

    # set schedule
    schedule.every(pipeline_config.get("schedule").get("run_seconds")).seconds.do(
        run_pipeline, pipeline_config=pipeline_config, postgresql_logging_client=postgresql_logging_client
    )

    while True: 
        schedule.run_pending()
        time.sleep(pipeline_config.get("schedule").get("poll_seconds"))