import pendulum
import logging

from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

from udacity.common import sql_statements

'''
This DAG is created a seperate DAG, because create table operation is a one time only operation, 
and we do not want to add this dependency step in the scheduled workflow
'''
@dag(
    start_date=pendulum.now()
)

def create_tables_in_redshift():

    @task
    def create_tables_in_redshift():
        create_redshift_tables = PostgresOperator(
            task_id="Create_tables",
            postgres_conn_id="redshift",
            sql="create_table_queries.sql"
        )
        logging.info("Creating Tables in RedShift in progress")
    create_tables_in_redshift()
create_tables_in_redshift_dag = create_tables_in_redshift()