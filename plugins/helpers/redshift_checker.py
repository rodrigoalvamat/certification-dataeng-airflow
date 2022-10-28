"""Defines the RedshiftChecker class to implement
the abstract validation methods inherited from
DataQualityChecker."""

# airflow libs
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLHook
# helpers libs
from data_quality_checker import DataQualityChecker


class RedshiftChecker(DataQualityChecker):
    """This class defines the data quality check methods
    that implement each validation rule as an SQL statement
    compatible with Redshift.
    """

    def __init__(self, rules, log, redshift_connection_id="redshift"):
        """Creates the RedshiftChecker object, and sets the rules, log,
        and the connection id.

        Args:
            rules: The rules JSON file absolute path.
            log: The airflow operator log object.
            redshift_connection_id: The Redshift connection name. Default: redshift.
        """
        super().__init__(rules, log)
        self.redshift = self.redshift = RedshiftSQLHook(redshift_conn_id=redshift_connection_id)

    def __get_nullable_count(self, table, column):
        """Gets the number of rows of a table's column with
        null values.

        Args:
            table: The name of the table.
            column: The name of the column.

        Returns:
            The number of rows with null values.
        """
        query = f"SELECT count(*) FROM {table} WHERE {column} IS NULL;"
        records = self.redshift.get_records(query)
        self.log.info(f"Nullable matches: {records}")
        return records[0][0]

    def __get_regexp_match_count(self, table, column, regexp):
        """Gets the number of rows of a table's column that match
        a PCRE regular expression rule.

        Args:
            table: The name of the table.
            column: The name of the column.
            regexp: The PCRE regular expression.

        Returns:
            The number of rows that match the regular expression.
        """
        query = f"SELECT sum(regexp_count({column}, '{regexp}', 1, 'p')) FROM {table};"
        records = self.redshift.get_records(query)
        self.log.info(f"Regexp matches: {records}")
        return records[0][0]

    def __get_timestamp_match_count(self, table, column):
        """Gets the number of rows of a table's column that
        have a timestamp value.

        Args:
            table: The name of the table.
            column: The name of the column.

        Returns:
            The number of rows that have a timestamp value.
        """
        query = f"SELECT sum(regexp_count(extract(epoch from {column}), '[[:digit:]]{10}', 1, 'p')) FROM {table};"
        records = self.redshift.get_records(query)
        self.log.info(f"Timestamp matches: {records}")
        return records[0][0]

    def __get_rule_match_count(self, table, column, rule):
        """Gets the number of rows of a table's column that
        matches the rule key and value defined by the JSON rules.

        Args:
            table: The name of the table.
            column: The name of the column.
            rule: A dictionary with the key and value of a rule.

        Returns:
            The number of rows validated by the rule.
        """
        rule_key = list(rule.keys())[0]
        rule_value = list(rule.values())[0]

        if rule_key == "regexp":
            return self.__get_regexp_match_count(table, column, rule_value)
        elif rule_key == "timestamp":
            return self.__get_timestamp_match_count(table, column)
        elif rule_key == "nullable":
            return self.__get_nullable_count(table, column)
        else:
            return self.get_table_row_count(table)

    def get_table_row_count(self, table):
        """Gets the total number of rows of a table.

        Args:
            table: The name of the table.

        Returns:
            The number of rows of a table.
        """
        query = f"SELECT count (*) from {table};"
        records = self.redshift.get_records(query)
        self.log.info(f"Table rows: {records}")
        return records[0][0]

    def get_valid_row_count(self, table, column, rules):
        """Gets the sum of valid rows that match each rule
        defined for a table's column.

        Args:
            table: The name of the table.
            column: The name of the column.
            rules: An array of rule objects.

        Returns:
            The sum of valid rows that match each rule.
        """
        valid_rows = 0
        for rule in rules:
            valid_rows += self.__get_rule_match_count(table, column, rule)
        return valid_rows
