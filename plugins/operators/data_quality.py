"""Defines the DataQualityOperator class to execute the
RedshiftChecker with the rules specified by the JSON file.
The check report is exported as a JSON string to a XCom.
"""

# airflow libs
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
# helpers libs
from helpers.redshift_checker import RedshiftChecker


class DataQualityOperator(BaseOperator):
    """This class defines the custom data quality operator
    to check the data inserted into the fact and dimensions
    tables.
    """

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, rules, *args, **kwargs):
        """Creates the DataQualityOperator object, and initializes
        the RedshiftChecker.

        Args:
            rules: The rules JSON file absolute path.
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_checker = RedshiftChecker(rules, self.log)

    def execute(self, context):
        """Executes the Redshift check process and exports the
        resulting report to a XCom key.

        Args:
            context: The task context inherited from BaseOperator.
        """
        report = self.redshift_checker.check()
        context['ti'].xcom_push(key='quality_check', value=report)
