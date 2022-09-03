"""Write a pandas dataframe to a database table"""


# from io import StringIO

import pandas as pd

# import pymysql as pms
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base

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


def _get_column_info(sa_session, table_name: str, dbname: str):

    query = f"SELECT * FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='{dbname}' AND `TABLE_NAME`='{table_name}';"
    session = sa_session.execute(query)
    cursor = session.cursor
    cols = [detail[0] for detail in cursor.description]
    info = cursor.fetchall()
    data = pd.DataFrame(info, columns=cols)

    return data


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


def _check_null(data: pd.DataFrame, info: pd.DataFrame):
    columns = info["COLUMN_NAME"].to_numpy()
    nullable_status = info["IS_NULLABLE"].to_numpy()

    for column, status in zip(columns, nullable_status):
        if status == "NO":
            if data[column].dropna().shape[0] < data.shape[0]:
                raise ValueError(f"`{column}` is non-nullable but has null value")

    data = data[columns].copy()

    return data


def form_sql_query(data: pd.DataFrame, info: pd.DataFrame, table_name: str):
    pass


def _create_new_table(data: pd.DataFrame):
    pass


def write_df_to_db(
    data: pd.DataFrame,
    host: str,
    dbname: str,
    user: str,
    password: str,
    port: int,
    table_name: str,
    create_table: bool = False,
):
    # connection = _get_mysql_connection(
    #     host=host, dbname=dbname, user=user, password=password, port=port
    # )
    engine = _get_sql_alchemy_engine(
        dialect="mysql", host=host, user=user, dbname=dbname, password=password, port=port
    )
    sa_session = Session(engine)

    _get_column_info(sa_session=sa_session, table_name=table_name, dbname=dbname)
    # info = _get_column_info(cursor=connection.cursor(), table_name=table_name, dbname=dbname)
    if create_table:
        query = f"DROP TABLE IF EXISTS {table_name}"
        sa_session.execute(query)
