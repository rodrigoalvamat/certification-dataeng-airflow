"""Defines the DataQualityChecker class to apply custom
validators for each table column according to the rules
specified by the JSON file."""

# abstract libs
from abc import ABC, abstractmethod
# json libs
import json


class DataQualityChecker(ABC):
    """This class defines the data quality check process
    to iterate over all table columns and apply the
    validation rules.
    You must extend the class and implement its abstract
    methods according to the target database.
    """

    def __init__(self, rules, log):
        """Creates the DataQualityChecker object, and sets the rules and log.

        Args:
            rules: The rules JSON file absolute path.
            log: The airflow operator log object.
        """
        self.rules = self.__load_json(rules)
        self.log = log

    def check(self):
        """Iterate over all tables to apply column validation rules.

        Returns:
            A JSON string reporting all table columns checks results.
        """
        report = {}

        for table in self.rules:
            table_rows = self.get_table_row_count(table["name"])
            report[table["name"]] = self.__check_columns(table["name"], table["columns"], table_rows)

        return json.dumps(report)

    def __check_columns(self, table, columns, rows):
        """Iterate over all columns of a table to apply validation rules.
        Raises an exception if the number of valid rows is different from
        the total of the table's rows.

        Returns:
            A dictionary reporting all column checks for a table.
        """

        report = {}

        for column in columns:
            valid_rows = self.get_valid_row_count(table, column["name"], column["rules"])

            if rows == valid_rows:
                message = f"Data quality on column {column['name']} from table {table} check passed."
                self.log.info(message)
            else:
                message = f"Data quality check failed. {table} contain invalid records"
                ValueError(message)

            report[column["name"]] = message

        return report

    @abstractmethod
    def get_table_row_count(self, table):
        """Abstract method to get the total rows of a table.
        Must be implemented by the child class.

        Args:
            table: The name of the table.

        Returns:
            The number of rows of a table.
        """
        pass

    @abstractmethod
    def get_valid_row_count(self, table, column, rules):
        """Abstract method to get the total rows of a table
        that comply with the validation rules for given a column.
        Must be implemented by the child class.

        Args:
            table: The name of the table.
            column: The column name to check the rules.
            rules: An array of rules to validate the column.

        Returns:
            The number of valid table rows for the given column.
        """
        pass

    @staticmethod
    def __load_json(rules):
        """Static method to load the validation rules from
        the JSON file.

        Args:
            rules: The rules JSON file absolute path.

        Returns:
            A dictionary object with all validation rules.
        """
        json_file = open(rules)
        data = json.load(json_file)
        json_file.close()
        return data
