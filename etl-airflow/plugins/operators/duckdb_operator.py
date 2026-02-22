from airflow.models import BaseOperator
from duckdb_provider.hooks.duckdb_hook import DuckDBHook


class DuckDBOperator(BaseOperator):

    def __init__(self, sql: str, duckdb_conn_id: str = 'duckdb_default', **kwargs):
        super().__init__(**kwargs)
        self.sql = sql
        self.duckdb_conn_id = duckdb_conn_id

    def execute(self, context):
        self.log.info(f"Executing SQL: {self.sql}")
        hook = DuckDBHook(duckdb_conn_id=self.duckdb_conn_id)
        conn = hook.get_conn()
        conn.sql(self.sql)
        conn.close()
        self.log.info("SQL executed successfully")
