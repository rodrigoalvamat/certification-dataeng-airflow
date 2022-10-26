from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLHook
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="aws_credentials",
                 redshift_connection_id="redshift",
                 s3_bucket="",
                 s3_key="",
                 table="",
                 sql_copy="",
                 sql_create="",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.aws_credentials_id = aws_credentials_id
        self.redshift_connection_id = redshift_connection_id
        self.s3_path = f"s3://{s3_bucket}/{s3_key}"

        self.table = table
        self.sql_copy = sql_copy
        self.sql_create = sql_create

    def execute(self, context):
        aws_hook = AwsBaseHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = RedshiftSQLHook(redshift_conn_id=self.redshift_connection_id)

        self.log.info("Drop and create Redshift table")
        redshift.run("DROP TABLE IF EXISTS {}".format(self.table), autocommit=True)
        redshift.run(self.sql_create, autocommit=True)

        self.log.info("Copying data from S3 to Redshift")

        sql_copy = self.sql_copy.format(
            self.s3_path,
            credentials.access_key,
            credentials.secret_key,
            aws_hook.region_name
        )
        redshift.run(sql_copy, autocommit=True)
