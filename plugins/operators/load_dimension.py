"""Defines the LoadDimensionOperator class to execute the
SQL insert statements for the dimension tables.
"""

# airflow libs
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLHook
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """This class defines the dimension table's data loader operator
    to execute drop, create and insert statements."""

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_connection_id="redshift",
                 table="",
                 append_only=False,
                 sql_create="",
                 sql_insert="",
                 *args, **kwargs):
        """Creates the LoadDimensionOperator object, and initializes
        the execution options.

        Args:
            redshift_connection_id: The Redshift connection name. Default: redshift.
            table: The name of the table.
            append_only: Drop and create table if false. Default: false.
            sql_create: The SQL create statement.
            sql_insert: The SQL insert statement.
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_connection_id = redshift_connection_id
        self.table = table
        self.append_only = append_only
        self.sql_create = sql_create
        self.sql_insert = sql_insert

    def execute(self, context):
        """Executes the RedshiftSQLHook for each SQL statement
        according to the options set by the constructor.

        Args:
            context: The task context inherited from BaseOperator.
        """
        redshift = RedshiftSQLHook(redshift_conn_id=self.redshift_connection_id)

        if not self.append_only:
            self.log.info("Drop and create Redshift table")
            redshift.run("DROP TABLE IF EXISTS {}".format(self.table), autocommit=True)
            redshift.run(self.sql_create, autocommit=True)

        self.log.info("Copying data from staging to dimension table")
        redshift.run(self.sql_insert, autocommit=True)
