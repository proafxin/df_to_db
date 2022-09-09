"""Write a pandas dataframe to a database table"""


import pandas as pd
from pandas.api.types import is_integer_dtype, is_numeric_dtype

# import pymysql as pms
from sqlalchemy import (
    Column,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    text,
)
from sqlalchemy.orm import Session

DIALECTS = {
    "sqlserver": "mssql",
    "mysql": "mysql",
    "postgresql": "postgresql",
}
DRIVERS = {
    "sqlserver": "pyodbc",
    "mysql": "mysqldb",
    "postgresql": "psycopg2",
}


def get_names(names: list[str]):
    """Get first element of name tuples
    Typically returned from a database query
    For example, result of `SHOW DATABASES;`

    :param names: Names to parse
    :type names: list[str]
    :return: List of names
    :rtype: `list[str]`
    """

    return [name[0] for name in names]


class SQLDatabaseConnection:
    """Database connection object for SQL databases"""

    def __init__(
        self,
        host: str = "localhost",
        dbname: str = "",
        user: str = "root",
        password: str = "",
        port: int = 3306,
    ) -> None:
        self.create_database(
            host=host,
            dbname=dbname,
            user=user,
            password=password,
            port=port,
        )
        self.__engine = self._get_sql_alchemy_engine(
            dialect="mysql",
            host=host,
            dbname=dbname,
            user=user,
            password=password,
            port=port,
        )

    def _get_sql_alchemy_engine(
        self, dialect: str, host: str, user: str, password: str, dbname: str, port: int
    ):

        assert dialect in DIALECTS

        driver = DRIVERS[dialect]
        dialect = DIALECTS[dialect]

        connection_string = f"{dialect}+{driver}://{user}:{password}@{host}:{port}"
        if dbname:
            connection_string = f"{connection_string}/{dbname}"
        engine = create_engine(connection_string, future=True)

        return engine

    def execute_single_query(self, query: str):
        """Execute a single query using this connection

        :param query: SQL statement to execute
        :type query: `str`
        :return: Result of executed query
        :rtype: `sqlalchemy.engine.cursor.CursorResult`
        """

        assert self.__engine is not None

        with self.__engine.connect() as conn:
            result = conn.execute(text(query))
            conn.commit()

            return result

    def create_database(
        self,
        host: str = "localhost",
        dbname: str = "",
        user: str = "root",
        password: str = "",
        port: int = 3306,
    ):
        """Create database `dbname`

        :param host: Host address of the database, defaults to "localhost"
        :type host: `str`, optional
        :param dbname: Name of the database, defaults to ""
        :type dbname: `str`, optional
        :param user: Database username, defaults to "root"
        :type user: `str`, optional
        :param password: Password of the database connection, defaults to ""
        :type password: `str`, optional
        :param port: Port to be used for this database connection, defaults to 3306
        :type port: `int`, optional
        """

        engine = self._get_sql_alchemy_engine(
            dialect="mysql",
            host=host,
            dbname=None,
            user=user,
            password=password,
            port=port,
        )
        with engine.connect() as conn:
            query = f"CREATE DATABASE IF NOT EXISTS {dbname}"

            conn.execute(text(query))
            conn.commit()

    def get_list_of_database(self):
        """Get list of database for this connection

        :return: List of string containing database names
        :rtype: `list[str]`
        """

        query = "SHOW DATABASES;"
        database_names = self.execute_single_query(query=query)
        database_names = get_names(names=database_names)

        return database_names

    def get_list_of_tables(self, dbname: str):
        """Get list of tables in database `dbname`

        :return: List of string containing tables names in the database
        :rtype: `list[str]`
        """

        query = f"""SHOW TABLES FROM {dbname}"""
        table_names = self.execute_single_query(query=query)
        table_names = get_names(names=table_names)

        return table_names

    def get_column_info(self, sa_session: Session, dbname: str, table_name: str):
        """Get table schema from database

        :param sa_session: SQLAlchemy Engine Session of the database connection
        :type sa_session: `sqlalchemy.orm.Session`
        :param dbname: Name of database
        :type dbname: `str`
        :param table_name: Name of the table in database
        :type table_name: `str`
        :return: Pandas dataframe of table schema
        :rtype: `pd.DataFrame`
        """

        query = "SELECT * FROM `INFORMATION_SCHEMA`.`COLUMNS`"
        query = f"{query} WHERE `TABLE_SCHEMA`='{dbname}' AND `TABLE_NAME`='{table_name}';"
        session = sa_session.execute(query)
        cursor = session.cursor
        cols = [detail[0] for detail in cursor.description]
        info = cursor.fetchall()

        data = pd.DataFrame(info, columns=cols)

        return data

    def has_table(self, table_name: str):
        """Check if the current database has table `table_name`

        :param table_name: Name of the table to check
        :type table_name: `str`
        :return: True if `table_name` exists in current database
        :rtype: `bool`
        """

        return self.__engine.dialect.has_table(
            connection=self.__engine.connect(), table_name=table_name
        )

    def _check_null(self, data: pd.DataFrame, info: pd.DataFrame):
        columns = info["COLUMN_NAME"].to_numpy()
        nullable_status = info["IS_NULLABLE"].to_numpy()
        column_keys = info["COLUMN_KEY"].to_numpy()

        columns_valid = []
        for column, status, key in zip(columns, nullable_status, column_keys):
            if "PRI" in key:
                continue
            if status == "NO":
                if data[column].dropna().shape[0] < data.shape[0]:
                    raise ValueError(f"`{column}` is non-nullable but has null value")
            columns_valid.append(column)

        data = data[columns_valid].copy()
        data = data.reset_index(drop=True)

        return data

    def clean_column(self, column: str):
        """Clean name of dataframe column

        :param column: Name of column
        :type column: `str`
        :return: Cleaned name of column
        :rtype: `str`
        """

        return str(column).strip().strip('"')

    def _clean_columns(self, data: pd.DataFrame):
        """Clean the column names of the dataframe.

        :param data: DataFrame to clean.
        :type data: pd.DataFrame
        :return: Cleaned DataFrame
        :rtype: `pd.DataFrame`
        """

        data.columns = [self.clean_column(column) for column in data.columns]

        return data

    def _get_table_from_dataframe(
        self,
        data: pd.DataFrame,
        table_name: str,
        primary_key: str = "id",
        max_length: int = 100,
    ):
        """Get SQLAlchemy Table from dataframe

        :param data: DataFrame with actual data
        :type data: pd.DataFrame
        :param engine: SQLAlchemy Engine of database connection
        :type engine: Engine
        :param table_name: Name of table
        :type table_name: str
        :param primary_key: Primary key of table, defaults to "id"
        :type primary_key: str, optional
        :param max_length: Maximum length of varchar, defaults to 100
        :type max_length: int, optional
        :return: SQLAlchemy Table of `data`
        :rtype: `sqlalchemy.Table`
        """

        metadata = MetaData(self.__engine)
        columns = []
        if primary_key in data.columns:
            data = data.drop(primary_key, axis=1)

        columns.append(Column(primary_key, Integer, primary_key=True, nullable=False))
        for column in data.columns:
            nullable_status = False
            if data[column].dropna().shape[0] < data.shape[0]:
                nullable_status = True

            if is_integer_dtype(data[column]):
                columns.append(Column(column, Integer, nullable=nullable_status))
            elif is_numeric_dtype(data[column]):
                columns.append(Column(column, Float, nullable=nullable_status))
            else:
                columns.append(Column(column, String(max_length), nullable=nullable_status))

        table = Table(table_name, metadata, *columns)

        return table

    def _create_new_table(self, table: Table):

        table.create(bind=self.__engine, checkfirst=True)

        return table

    def _write_data_to_table(self, data: pd.DataFrame, table: Table):

        """Write `data` to `table` using connection from `engine`

        :return: Result with rows written to table
        :rtype: `sqlalchemy.engine.cursor.CursorResult`
        """

        records = data.to_dict("records")

        with self.__engine.connect() as conn:
            ins = table.insert()

            result = conn.execute(ins, records)
            conn.commit()

            return result

    def delete_table(self, table_name: str):
        """Drop table `table_name` from the current database if it exists

        :param table: Table to delete
        :type table: `Table`
        """

        query = f"DROP TABLE IF EXISTS {table_name}"
        with self.__engine.connect() as conn:
            conn.execute(text(query))
            conn.commit()

    def write_df_to_db(
        self,
        data: pd.DataFrame,
        dbname: str,
        table_name: str,
        primary_key: str = "id",
        drop_first: bool = False,
        clean_columns: bool = True,
        max_length: int = 100,
    ):
        """Write `data` to Table `table_name`

        :param data: Pandas dataframe containing data to write
        :type data: pd.DataFrame
        :param dbname: Name of the database
        :type dbname: str
        :param table_name: Name of table in the database
        :type table_name: str
        :param primary_key: Primary key of table, defaults to "id"
        :type primary_key: str, optional
        :param drop_first: if True, table `table_name` in database will be attempted to drop first.
        :type drop_first: bool
        :param clean_columns: If True, trailing/leading whitespaces and " will be stripped
            off column names, defaults to "True"
        :type clean_columns: bool
        :return: Cursor with result of query execution
        :rtype: CursorResult
        """

        if clean_columns:
            data = self._clean_columns(data=data)

        data = data.astype(object).where(pd.notnull(data), None)

        engine = self.__engine
        sa_session = Session(engine)

        # _get_column_info(sa_session=sa_session, table_name=table_name, dbname=dbname)
        # info = _get_column_info(cursor=connection.cursor(), table_name=table_name, dbname=dbname)
        table = self._get_table_from_dataframe(
            data=data,
            table_name=table_name,
            primary_key=primary_key,
            max_length=max_length,
        )

        if drop_first:
            self.delete_table(table_name=table_name)

        table = self._create_new_table(table=table)
        info = self.get_column_info(sa_session=sa_session, table_name=table_name, dbname=dbname)
        data = self._check_null(data=data, info=info)
        result = self._write_data_to_table(data=data, table=table)

        return result, table

    def close_connection(self):
        """Close the current connection to the database"""

        self.__engine.dispose()
