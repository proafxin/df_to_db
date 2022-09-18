"""Test dataframe to database module"""

import os
import random
from io import StringIO

import numpy as np
import pandas as pd
from requests import get
from sqlalchemy.engine.cursor import CursorResult
from write_df.sql_writer import SQLDatabaseConnection

DBNAME = "__test_db__"

MYSQL_CONNECTION = SQLDatabaseConnection(
    dbtype="mysql",
    host=os.environ["MYSQL_HOST"],
    dbname=DBNAME,
    user=os.environ["MYSQL_USER"],
    password=os.environ["MYSQL_PASSWORD"],
    port=os.environ["MYSQL_PORT"],
)

POSTGRE_CONNECTION = SQLDatabaseConnection(
    dbtype="postgresql",
    host=os.environ["POSTGRESQL_HOST"],
    dbname=DBNAME,
    user=os.environ["POSTGRESQL_USER"],
    password=os.environ["POSTGRESQL_PASSWORD"],
    port=os.environ["POSTGRESQL_PORT"],
)
SQLSERVER_CONNECTION = SQLDatabaseConnection(
    dbtype="sqlserver",
    host=os.environ["SQLSERVER_HOST"],
    dbname=DBNAME,
    user=os.environ["SQLSERVER_USER"],
    password=os.environ["SQLSERVER_PASSWORD"],
    port=os.environ["SQLSERVER_PORT"],
)

CONNECTIONS = [
    ("mysql", {"conn": MYSQL_CONNECTION}),
    ("sqlserver", {"conn": SQLSERVER_CONNECTION}),
    ("postgresql", {"conn": POSTGRE_CONNECTION}),
]


def pytest_generate_tests(metafunc):
    """Generate pytest Tests for all connections

    :param metafunc: _description_
    :type metafunc: _type_
    """

    idlist = []
    argvalues = []
    for scenario in metafunc.cls.connections:
        idlist.append(scenario[0])
        items = scenario[1].items()
        argnames = [x[0] for x in items]
        argvalues.append([x[1] for x in items])
    metafunc.parametrize(argnames, argvalues, ids=idlist, scope="class")
    for connection in CONNECTIONS:
        connection[1]["conn"].close_connection()


class TestWriteToSQL:
    """Test class for writing to MySQL database"""

    __dbname = DBNAME
    connections = CONNECTIONS

    def test_create_database(self, conn: SQLDatabaseConnection):
        """Test if database is indeed created"""

        database_names = conn.get_list_of_database()
        assert self.__dbname in database_names

    def test_write_without_primary_key_no_null(self, conn: SQLDatabaseConnection):
        """Test writing dataframe without primary key"""

        response = get(url="https://people.sc.fsu.edu/~jburkardt/data/csv/cities.csv")
        assert response.status_code == 200

        data = pd.read_csv(StringIO(response.content.decode()))
        table_name = "test__table__"
        result = conn.write_df_to_db(
            data=data,
            table_name=table_name,
            drop_first=True,
        )
        assert conn.has_table(table_name=table_name) is True
        assert isinstance(result, CursorResult)
        assert result.rowcount == data.shape[0]
        conn.delete_table(table_name=table_name)

    def test_write_with_primary_key_and_float(self, conn: SQLDatabaseConnection):
        """Test writing dataframe with primary key and float data"""

        response = get(url="https://people.sc.fsu.edu/~jburkardt/data/csv/cities.csv")
        assert response.status_code == 200

        data = pd.read_csv(StringIO(response.content.decode()))
        primary_key = "id"
        data[primary_key] = [random.randint(1, data.shape[0]) for i in range(data.shape[0])]
        data["y"] = [random.random() for i in range(data.shape[0])]
        table_name = "test__table__"

        result = conn.write_df_to_db(
            data=data,
            table_name=table_name,
            drop_first=True,
        )
        assert conn.has_table(table_name=table_name) is True
        assert isinstance(result, CursorResult)
        assert result.rowcount == data.shape[0]
        conn.delete_table(table_name=table_name)

    def test_write_with_primary_key_null(self, conn: SQLDatabaseConnection):
        """Test writing dataframe without primary key"""

        response = get(url="https://people.sc.fsu.edu/~jburkardt/data/csv/cities.csv")
        assert response.status_code == 200

        data = pd.read_csv(StringIO(response.content.decode()))
        data["test"] = [random.randint(1, data.shape[0]) for i in range(data.shape[0])]
        data["test"] = data["test"].astype(int)
        data["y"] = [random.random() for i in range(data.shape[0])]
        data.at[0, "y"] = np.nan
        table_name = "test__table__"

        result = conn.write_df_to_db(
            data=data,
            table_name=table_name,
            id_col=None,
            drop_first=True,
        )
        assert isinstance(result, CursorResult)
        assert result.rowcount == data.shape[0]
        conn.delete_table(table_name=table_name)
