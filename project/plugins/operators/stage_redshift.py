from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    COPY_SQL = """
        COPY {} 
        FROM '{}' 
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS json '{}';
    """
    
    @apply_defaults
    def __init__(self,
                conn_id="redshift",
                aws_credentials_id="aws_credentials",
                table="",
                s3_bucket="",
                s3_key="",
                log_json_file="",
                *args, **kwargs):
        """ Initialize the RedShift Operator with default arguments for redshift and aws_credentials connections """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_file = log_json_file
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        """ Executor for Staging RedShift Tables """
        self.log.info("Redshift Connection Setup Start")
        metastoreBackend = MetastoreBackend()
        aws_creds=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info("Redshift Connection Established")

        self.log.info("Truncate any existing data from Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift Table")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)


        if self.log_json_file is not None and self.log_json_file != "":
            copy_sql_formatted = StageToRedshiftOperator.COPY_SQL.format(
                self.table,
                s3_path,
                aws_creds.login,
                aws_creds.password,
                self.log_json_file)
        else:
            copy_sql_formatted = StageToRedshiftOperator.COPY_SQL.format(
                self.table,
                s3_path,
                aws_creds.login,
                aws_creds.password,
                'auto')

        self.log.info(f"Start RedShift Copy operation {self.table}")
        redshift.run(copy_sql_formatted)
        self.log.info(f"Successfully loaded RedShift table {self.table}")
