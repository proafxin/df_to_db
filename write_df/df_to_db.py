"""Write a pandas dataframe to a database table"""

from typing import Any

import pandas as pd
import pymysql as pms

__all__ = ["write_df_to_db"]


def _get_mysql_connection(host: str, dbname: str, user: str, password: str, port: int):
    """_summary_

    :param host: Host of the database.
    :type host: str
    :param dbname: Name of the databse.
    :type dbname: str
    :param user: Username of the database account.
    :type user: str
    :param password: Password of the database account.
    :type password: str
    :param port: Port of the database connection.
    :type port: int
    :return: Connection to the MySQL database.
    :rtype: pymysql.connect
    """

    connection = pms.connect(
        host=host,
        db=dbname,
        user=user,
        password=password,
        port=port,
    )

    return connection


def _get_column_info(cursor: Any, table_name: str, dbname: str):
    query = f"SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='{dbname}' AND `TABLE_NAME`='{table_name}';"
    cursor.execute(query)
    info = cursor.fetchall()

    print(info)


def form_sql_query(data: pd.DataFrame, conn: Any, table_name: str):
    pass


def write_df_to_db(
    data: pd.DataFrame, host: str, dbname: str, user: str, password: str, port: int, table_name: str
):
    connection = _get_mysql_connection(
        host=host, dbname=dbname, user=user, password=password, port=port
    )
    _get_column_info(cursor=connection.cursor(), table_name=table_name, dbname=dbname)
