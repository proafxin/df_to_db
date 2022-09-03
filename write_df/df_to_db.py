"""Write a pandas dataframe to a database table"""


# from io import StringIO

import pandas as pd
from pandas.api.types import is_integer_dtype, is_numeric_dtype

# import pymysql as pms
from sqlalchemy import Column, Float, Integer, MetaData, String, Table, create_engine
from sqlalchemy.engine import Engine

# from sqlalchemy.orm import Session, declarative_base

__all__ = ["write_df_to_db"]


# def _get_mysql_connection(host: str, dbname: str, user: str, password: str, port: int):
#     """Get MySQL connection from credentials.

#     :param host: Host of the database.
#     :type host: str
#     :param dbname: Name of the databse.
#     :type dbname: str
#     :param user: Username of the database account.
#     :type user: str
#     :param password: Password of the database account.
#     :type password: str
#     :param port: Port of the database connection.
#     :type port: int
#     :return: Connection to the MySQL database.
#     :rtype: pymysql.connect
#     """

#     connection = pms.connect(
#         host=host,
#         db=dbname,
#         user=user,
#         password=password,
#         port=port,
#     )

#     return connection


# def _get_column_info(sa_session, table_name: str, dbname: str):

#     query = "SELECT * FROM `INFORMATION_SCHEMA`.`COLUMNS`"
#     query = f"{query} WHERE `TABLE_SCHEMA`='{dbname}' AND `TABLE_NAME`='{table_name}';"
#     session = sa_session.execute(query)
#     cursor = session.cursor
#     cols = [detail[0] for detail in cursor.description]
#     info = cursor.fetchall()
#     data = pd.DataFrame(info, columns=cols)

#     return data


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


def _get_sql_alchemy_engine(
    dialect: str, host: str, user: str, password: str, dbname: str, port: int
):

    assert dialect in DIALECTS

    driver = DRIVERS[dialect]
    dialect = DIALECTS[dialect]

    connection_string = f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{dbname}"
    engine = create_engine(connection_string, future=True)

    return engine


# def _check_null(data: pd.DataFrame, info: pd.DataFrame):
#     columns = info["COLUMN_NAME"].to_numpy()
#     nullable_status = info["IS_NULLABLE"].to_numpy()

#     for column, status in zip(columns, nullable_status):
#         if status == "NO":
#             if data[column].dropna().shape[0] < data.shape[0]:
#                 raise ValueError(f"`{column}` is non-nullable but has null value")

#     data = data[columns].copy()

#     return data


# def form_sql_query(data: pd.DataFrame, info: pd.DataFrame, table_name: str):
#     pass


def clean_column(column: str):
    """Clean name of dataframe column

    :param column: Name of column
    :type column: str
    :return: Cleaned name of column
    :rtype: str
    """

    return str(column).strip().strip('"')


def _get_table_from_dataframe(
    data: pd.DataFrame,
    engine: Engine,
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

    assert engine is not None, "engine must be present"

    metadata = MetaData(engine)
    columns = []
    if primary_key in data.columns:
        data = data.drop(primary_key, axis=1)

    columns.append(Column(primary_key, Integer, primary_key=True))
    for column in data.columns:
        clean_name = clean_column(column=column)
        if is_integer_dtype(data[column]):
            columns.append(Column(clean_name, Integer))
        elif is_numeric_dtype(data[column]):
            columns.append(Column(clean_name, Float))
        else:
            columns.append(Column(clean_name, String(max_length)))

    table = Table(table_name, metadata, *columns)

    return table


def _create_new_table(table: Table, engine: Engine):

    table.create(bind=engine, checkfirst=True)

    return table


def _write_data_to_table(data: pd.DataFrame, engine: Engine, table: Table):

    """Write `data` to `table` using connection from `engine`

    :return: Result with rows written to table
    :rtype: sqlalchemy.engine.cursor.CursorResult
    """

    data.columns = [clean_column(column=column) for column in data.columns]

    with engine.connect() as conn:
        ins = table.insert()
        records = data.to_dict("records")

        result = conn.execute(ins, records)
        conn.commit()

        return result


def _delete_table(table: Table, engine: Engine):

    table.drop(bind=engine, checkfirst=True)


def write_df_to_db(
    data: pd.DataFrame,
    host: str,
    dbname: str,
    user: str,
    password: str,
    port: int,
    table_name: str,
    primary_key: str = "id",
    drop_first: bool = False,
    max_length: int = 100,
):
    """Write `data` to Table `table_name`

    :param data: Pandas dataframe containing data to write
    :type data: pd.DataFrame
    :param host: Host URL
    :type host: str
    :param dbname: Name of database
    :type dbname: str
    :param user: Username of this database connection
    :type user: str
    :param password: Password of this database connection
    :type password: str
    :param port: Port of this database connection
    :type port: int
    :param table_name: Name of table
    :type table_name: str
    :param primary_key: Primary key of table, defaults to "id"
    :type primary_key: str, optional
    :return: Cursor with result of query execution
    :rtype: CursorResult
    """

    engine = _get_sql_alchemy_engine(
        dialect="mysql", host=host, user=user, dbname=dbname, password=password, port=port
    )
    # sa_session = Session(engine)

    # _get_column_info(sa_session=sa_session, table_name=table_name, dbname=dbname)
    # info = _get_column_info(cursor=connection.cursor(), table_name=table_name, dbname=dbname)
    table = _get_table_from_dataframe(
        data=data,
        engine=engine,
        table_name=table_name,
        primary_key=primary_key,
        max_length=max_length,
    )
    if drop_first:
        _delete_table(table=table, engine=engine)
    table = _create_new_table(table=table, engine=engine)
    result = _write_data_to_table(data=data, engine=engine, table=table)

    return result, table
