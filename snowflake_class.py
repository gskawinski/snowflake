import os
import snowflake.connector

# https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-example
# https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api#label-snowflake-connector-methods
# https://github.com/snowflakedb/snowflake-connector-python
# pip install --upgrade snowflake-connector-python
# https://toppertips.com/snowflake-python-connector
# https://www.geeksforgeeks.org/how-to-convert-pandas-dataframe-into-sql-in-python/
from dotenv import load_dotenv

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import json

# Load .env file
load_dotenv()

# snowflake class client


class SnowflakeTable:

    def __init__(self, user, password, account, warehouse, database, schema):
        try:
            self.connection = snowflake.connector.connect(
                user=user,
                password=password,
                account=account,
                warehouse=warehouse,
                database=database,
                schema=schema,
            )
            self.database = database
            self.schema = schema
            print(f"Connected to Snowflake DB: {database} successfully.")

        except snowflake.connector.errors.OperationalError as e:
            print("Error connecting to Snowflake:", e)

    # fetch all data from table
    def fetch_data(self, table, pandas=False):
        # if pandas then return pandas.DataFrame
        query = f"SELECT * FROM {table}"
        if pandas:
            return pd.DataFrame(self.execute_query(query))
        else:
            return self.execute_query(query)

    # query DB
    def execute_query(self, query):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            return cursor.fetchall()
        except snowflake.connector.errors.ProgrammingError as e:
            print("Error executing query:", e)
            return None

    # insert one row to table using Binding
    def insert_single(self, data, table):
        # data is type: tuple (val1, val2 ....)
        try:
            cursor = self.connection.cursor()
            placeholders = ", ".join(["%s"] * len(data))
            # print(placeholders)
            insert_query = f"INSERT INTO {table} VALUES ({placeholders})"
            # print(insert_query)
            cursor.execute(insert_query, data)
            self.connection.commit()
            print("Single record inserted successfully.")
            return True

        except snowflake.connector.errors.ProgrammingError as e:
            print("Error inserting single record:", e)
            self.connection.rollback()
            return False

    # insert bulk/batch data into table
    def insert_batch(self, records, table):
        # data is type: list of tuple [ (val1, val2 ....), (val1, val2 ....) ]
        try:
            cursor = self.connection.cursor()
            placeholders = ", ".join(["%s"] * len(records[0]))
            insert_query = f"INSERT INTO {table} VALUES ({placeholders})"
            cursor.executemany(insert_query, records)
            self.connection.commit()
            print("Batch insert successful.")
            return True

        except snowflake.connector.errors.ProgrammingError as e:
            print("Error performing batch insert:", e)
            self.connection.rollback()
            return False

    # change current processed db or schema in snowflake
    def change_db_schema(self, database=False, schema=False):
        """
        Change the database/schema if database/schema names are given
        example. database=new_db_name or schema=new_schema_name
        """
        try:
            cursor = self.connection.cursor()
            query = f"USE DATABASE {database};" if database else ""
            query += f"USE SCHEMA {schema};" if schema else ""
            if query != "":
                cursor.execute(query)
                self.connection.commit()
                self.database = database
                self.schema = schema
                print(f"Database/schema: {database}/{schema} changed successfully.")

        except snowflake.connector.errors.ProgrammingError as e:
            print("Error changing database/schema:", e)
            self.connection.rollback()

    # list snowflak databases, schemas (in current DB) and tables (in current DB and chema)
    def list_db_sch_tb(self, obj2show):
        # show_to_show allowed (databases, schemas, tables)
        try:
            cursor = self.connection.cursor()

            query = f"SHOW {obj2show}"
            if query != "":
                cursor.execute(query)
                return [row[1] for row in cursor.fetchall()]
            else:
                return []
        except snowflake.connector.errors.ProgrammingError as e:
            print("Error listing :", e)

    # load from csv/xls
    def load_csv2table(self, table, file_path, **kwargs):
        try:
            # Create a DataFrame from files CSV/XLS
            if file_path.endswith(".csv"):
                df = pd.read_csv(file_path, **kwargs)
            elif file_path.endswith((".xls", ".xlsx")):
                df = pd.read_excel(file_path, **kwargs)
            else:
                raise ValueError("Unsupported file type")
            # print(df)
            # Convert DataFrame to a list of tuples
            records = [tuple(row) for row in df.values]
            # batch insert - list of tuples to Snowflake
            status = self.insert_batch(records, table)
            if status:
                print(f"File: {file_path} loaded to Snowflake table: {table} ")

        except snowflake.connector.errors.ProgrammingError as e:
            print("Error loading CSV data:", e)
            self.connection.rollback()

    def load_json2table(self, table, json_path):
        try:
            with open(json_path, "r") as json_file:
                data = json.load(json_file)

            cursor = self.connection.cursor()
            json_df = pd.json_normalize(data)
            # Convert DataFrame to a list of tuples
            records = [tuple(row) for row in df.values]
            # batch insert - list of tuples to Snowflake
            status = self.insert_batch(records, table)
            if status:
                print(
                    f"Data from JSON file: {json_path} loaded to Snowflake table: {table} "
                )

        except Exception as e:
            print("Error reading from JSON and inserting:", e)
            self.connection.rollback()

    # close connection to DB
    def close_connection(self):
        self.connection.close()
        print(f"Connection to DB: {self.database} closed.")


# ----------------
if __name__ == "__main__":

    # connecion parameters
    USER = os.getenv("SNOWSQL_USER")
    PASSWORD = os.getenv("SNOWSQL_PWD")
    ACCOUNT = os.getenv("SNOWSQL_ACCOUNT")
    WAREHOUSE = "COMPUTE_WH"
    DATABASE = "financial_assets_db"
    SCHEMA = "main_assets"

    # create snowflake object
    sfc = SnowflakeTable(USER, PASSWORD, ACCOUNT, WAREHOUSE, DATABASE, SCHEMA)

    TABLE = "ASSET_TYPES"
    query = "SELECT * FROM ASSET_TYPES"
    data = sfc.execute_query(query)
    print(data)

    # data = sfc.list_db_sch_tb('databases')
    # data = sfc.list_db_sch_tb('schemas')
    data = sfc.list_db_sch_tb("tables")
    print(data)

    # Insert a single record
    record = (1, "Index", "Financial indexes of markets/sectors.")
    sfc.insert_single(data=record, table=TABLE)

    # Batch insert records
    records = [
        (2, "Stock", "Shares of publicly-traded companies."),
        (3, "Real Estate", "Real Estate investing vehicules"),
        (4, "Currency", "Foreign currency FX exchange rate."),
    ]
    sfc.insert_batch(records, table=TABLE)

    # Fetch data from the table
    df_data = sfc.fetch_data(TABLE, pandas=True)
    # print(data)
    # df = pd.DataFrame(data)
    # print(df)

    # Save to CSV file
    csv_file_path = "data.csv"
    # df_data.to_csv(csv_file_path, index=False)  # Set index=False to exclude row numbers

    # Save to XLS (Excel) file
    xls_file_path = "data.xlsx"
    # df_data.to_excel(xls_file_path, index=False)  # Set index=False to exclude row numbers

    sfc.load_csv2table(TABLE, csv_file_path)

    # close connection
    sfc.close_connection()
