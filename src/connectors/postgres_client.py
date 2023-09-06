from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects import postgresql


class PostgreSqlClient:
    """
    A client for querying postgresql database. 
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

        connection_url = URL.create(
            drivername="postgresql",
            username=username,
            password=password,
            host=server_name,
            port=port,
            database=database_name,
        )

        self.engine = create_engine(connection_url)

    def write_to_table(self, data: list[dict], table: Table) -> None:
        metadata = MetaData()
        key_columns = [
            pk_column.name for pk_column in table.primary_key.columns.values()]
        metadata.create_all(self.engine)  # creates table if it does not exist
        insert_statement = postgresql.insert(table).values(data)
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=key_columns,
            set_={c.key: c for c in insert_statement.excluded if c.key not in key_columns})
        self.engine.execute(upsert_statement)
