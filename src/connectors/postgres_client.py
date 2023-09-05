from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql


class PostgreSqlClient:
    """
    A client for querying a PostgreSQL database.
    """

    def __init__(self,
                 server_name: str,
                 database_name: str,
                 username: str,
                 password: str,
                 port: int = 5432
                 ):
        self.host_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.port = port

        # Use the correct drivername "postgresql" here
        connection_url = URL.create(
            drivername="postgresql+pg8000",
            username='postgres',
            password='postgres',
            host="project1.c8jtsjfdp6ka.us-east-1.rds.amazonaws.com",  # Use "db" as the hostname
            port=5432,
            database="postgres",
        )

        self.engine = create_engine(connection_url)

    def write_to_table(self, data, table: Table, metadata: MetaData):
        key_columns = [
            pk_column.name for pk_column in table.primary_key.columns.values()]
        # creates the table if it does not exist
        metadata.create_all(self.engine)
        insert_statement = postgresql.insert(table).values(data)
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=key_columns,
            set_={c.key: c for c in insert_statement.excluded if c.key not in key_columns})
        self.engine.execute(upsert_statement)
