from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLHook


class RedshiftValidator:

    def __init__(self,
                 log,
                 redshift_connection_id="redshift"):
        self.log = log
        self.redshift = self.redshift = RedshiftSQLHook(redshift_conn_id=redshift_connection_id)

    def __get_nullable_count(self, table, column):
        query = f"SELECT count(*) FROM {table} WHERE {column} IS NULL;"
        records = self.redshift.get_records(query)
        self.log.info(f"Nullable matches: {records}")
        return records[0][0]

    def __get_regexp_match_count(self, table, column, regexp):
        query = f"SELECT sum(regexp_count({column}, '{regexp}', 1, 'p')) FROM {table};"
        records = self.redshift.get_records(query)
        self.log.info(f"Regexp matches: {records}")
        return records[0][0]

    def __get_timestamp_match_count(self, table, column):
        query = f"SELECT sum(regexp_count(extract(epoch from {column}), '[[:digit:]]{10}', 1, 'p')) FROM {table};"
        records = self.redshift.get_records(query)
        self.log.info(f"Timestamp matches: {records}")
        return records[0][0]

    def __get_rule_match_count(self, table, column, rule):
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
        query = f"SELECT count (*) from {table};"
        records = self.redshift.get_records(query)
        self.log.info(f"Table rows: {records}")
        return records[0][0]

    def get_valid_row_count(self, table, column, rules):
        valid_rows = 0
        for rule in rules:
            valid_rows += self.__get_rule_match_count(table, column, rule)
        return valid_rows
