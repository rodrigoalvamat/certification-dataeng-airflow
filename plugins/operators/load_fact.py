from airflow.models import BaseOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLHook
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_connection_id="redshift",
                 table="",
                 append_only=False,
                 sql_create="",
                 sql_insert="",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.redshift_connection_id = redshift_connection_id
        self.table = table
        self.append_only = append_only
        self.sql_create = sql_create
        self.sql_insert = sql_insert

    def execute(self, context):
        redshift = RedshiftSQLHook(redshift_conn_id=self.redshift_connection_id)

        if not self.append_only:
            self.log.info("Drop and create Redshift table")
            redshift.run("DROP TABLE IF EXISTS {}".format(self.table), autocommit=True)
            redshift.run(self.sql_create, autocommit=True)

        self.log.info("Copying data from staging to fact table")
        redshift.run(self.sql_insert, autocommit=True)
