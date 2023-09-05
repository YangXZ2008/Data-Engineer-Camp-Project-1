from jinja2 import Environment
from etl_project.connectors.postgresql import PostgreSqlClient
from graphlib import TopologicalSorter

class SqlTransform:
    def __init__(self, postgresql_client: PostgreSqlClient, environment: Environment, table_name: str):
        self.postgresql_client = postgresql_client
        self.environment = environment
        self.table_name = table_name
        self.template = self.environment.get_template(f"{table_name}.sql")
        
    def create_table_as(self) -> None:
        """
        Drops the table if it exists and creates a new copy of the table using the provided select statement. 
        """
        exec_sql = f"""
            drop table if exists {self.table_name};
            create table {self.table_name} as (
                {self.template.render()}
            )
        """
        self.postgresql_client.execute_sql(exec_sql)