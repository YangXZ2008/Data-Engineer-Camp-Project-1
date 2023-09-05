from sqlalchemy import create_engine, Table, MetaData, Column, inspect
from sqlalchemy.engine import URL
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
            drivername = "postgresql+pg8000", 
            username = username,
            password = password,
            host = server_name, 
            port = port,
            database = database_name, 
        )

        self.engine = create_engine(connection_url)

    def execute_sql(self, sql: str) -> None:
        self.engine.execute(sql)

    def create_table(self, metadata: MetaData) -> None:
        """
        Creates table provided in the metadata object
        """
        metadata.create_all(self.engine)

    def write_to_table(self, data: list[dict], table: Table, metadata: MetaData, loadtype:str='insert', chunk:int=1000 ) -> None:
        LOAD_TYPE = ['insert', 'chunk']
        metadata.create_all(self.engine) # creates table if it does not exist

        if loadtype not in  LOAD_TYPE:
            raise Exception(f"Try insert or chunk!")
        
        elif loadtype == LOAD_TYPE[0]:
            insert_statement = postgresql.insert(table).values(data)
            self.engine.execute(insert_statement)

        else:    
            key_columns = [pk_column.name for pk_column in table.primary_key.columns.values()]
            max_length = len(data)
            for i in range(0, max_length, chunk):
                if i + chunk >= max_length: 
                    lower_bound = i
                    upper_bound = max_length
                else: 
                    lower_bound = i 
                    upper_bound = i + chunk
                
                insert_statement = postgresql.insert(table).values(data[lower_bound:upper_bound])
                upsert_statement = insert_statement.on_conflict_do_update(
                    index_elements=key_columns,
                    set_={c.key: c for c in insert_statement.excluded if c.key not in key_columns})
                self.engine.execute(upsert_statement)