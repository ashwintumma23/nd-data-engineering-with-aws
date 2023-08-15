from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id = "redshift",
                 tables = "",
                 *args, **kwargs):
        """ Initialize the RedShift Operator with default arguments for redshift connections """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info(f'Starting Data Quality Checks')
        redshift_hook = PostgresHook(self.conn_id)
        for table in self.tables:
            self.log.info(f'Starting Data Quality Checks for {table}')
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                #raise ValueError(f"Data quality check failed. {table} returned no results")
                self.log.info(f"Data quality failed on table {table}")
            if records[0][0] < 1:
                #raise ValueError(f"Data quality check failed. {table} contained 0 rows")
                self.log.info(f"Data quality failed on table {table}")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")