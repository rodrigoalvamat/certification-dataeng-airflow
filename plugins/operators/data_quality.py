import json

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers.redshift_validator import RedshiftValidator


class DataQualityOperator(BaseOperator):
    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, json_rules, *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift = RedshiftValidator(self.log)
        self.rules = self.__json_load(json_rules)

    @staticmethod
    def __json_load(json_rules):
        json_file = open(json_rules)
        data = json.load(json_file)
        json_file.close()
        return data

    def __check_table_columns(self, table, columns, rows):
        report = {}
        for column in columns:
            valid_rows = self.redshift.get_valid_row_count(table, column["name"], column["rules"])

            if rows == valid_rows:
                message = f"Data quality on column {column['name']} from table {table} check passed."
                self.log.info(message)
            else:
                message = f"Data quality check failed. {table} contain invalid records"
                ValueError(message)

            report[column["name"]] = message

        return report

    def __check_tables(self):
        report = {}

        for table in self.rules:
            table_rows = self.redshift.get_table_row_count(table["name"])
            report[table["name"]] = self.__check_table_columns(table["name"], table["columns"], table_rows)

        return report

    def execute(self, context):
        report = json.dumps(self.__check_tables())
        context['ti'].xcom_push(key='quality_check', value=report)
