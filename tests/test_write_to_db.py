"""Test dataframe to database module"""

import os
import random
from io import StringIO

import numpy as np
import pandas as pd

# import pymysql as pms
from requests import get
from sqlalchemy.engine.cursor import CursorResult
from write_df.df_to_db import SQLDatabaseConnection

TABLE_NAMES = os.environ["TEST_TABLE_NAMES"].split(",")


class TestWriteToMySQL:
    """Test class for writing to MySQL database"""

    __dbname = "__testdb__"

    __dbconnobj = SQLDatabaseConnection(
        host=os.environ["MYSQL_HOST"],
        dbname=__dbname,
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        port=int(os.environ["MYSQL_PORT"]),
    )

    def test_create_database(self):
        """Test if database is indeed created"""

        database_names = self.__dbconnobj.get_list_of_database()
        assert self.__dbname in database_names

    def test_mysql_write_without_primary_key_no_null(self):
        """Test writing dataframe without primary key"""

        response = get(url="https://people.sc.fsu.edu/~jburkardt/data/csv/cities.csv")
        assert response.status_code == 200

        data = pd.read_csv(StringIO(response.content.decode()))
        table_name = "test__table__"
        result, _ = self.__dbconnobj.write_df_to_db(
            data=data,
            dbname=self.__dbname,
            table_name=table_name,
            primary_key="id",
            drop_first=True,
        )
        assert self.__dbconnobj.has_table(table_name=table_name) is True
        assert isinstance(result, CursorResult)
        assert result.rowcount == data.shape[0]
        self.__dbconnobj.delete_table(table_name=table_name)

    def test_mysql_write_with_primary_key_and_float(self):
        """Test writing dataframe with primary key and float data"""

        response = get(url="https://people.sc.fsu.edu/~jburkardt/data/csv/cities.csv")
        assert response.status_code == 200

        data = pd.read_csv(StringIO(response.content.decode()))
        primary_key = "id"
        data[primary_key] = [random.randint(1, data.shape[0]) for i in range(data.shape[0])]
        data["y"] = [random.random() for i in range(data.shape[0])]
        table_name = "test__table__"

        result, _ = self.__dbconnobj.write_df_to_db(
            data=data,
            dbname=self.__dbname,
            table_name=table_name,
            primary_key=primary_key,
            drop_first=True,
        )
        assert self.__dbconnobj.has_table(table_name=table_name) is True
        table_names = self.__dbconnobj.get_list_of_tables(dbname=self.__dbname)
        assert table_name in table_names
        assert isinstance(result, CursorResult)
        assert result.rowcount == data.shape[0]
        self.__dbconnobj.delete_table(table_name=table_name)

    def test_mysql_write_without_primary_key(self):
        """Test writing dataframe without primary key"""

        response = get(url="https://people.sc.fsu.edu/~jburkardt/data/csv/cities.csv")
        assert response.status_code == 200

        data = pd.read_csv(StringIO(response.content.decode()))
        primary_key = "id"
        data[primary_key] = [random.randint(1, data.shape[0]) for i in range(data.shape[0])]
        data["y"] = [random.random() for i in range(data.shape[0])]
        data.at[0, "y"] = np.nan
        data.to_csv("data.csv", index=False)
        data = pd.read_csv("data.csv")
        table_name = "test__table__"

        result, _ = self.__dbconnobj.write_df_to_db(
            data=data,
            dbname=self.__dbname,
            table_name=table_name,
            primary_key=primary_key,
            drop_first=True,
        )
        assert self.__dbconnobj.has_table(table_name=table_name) is True
        assert isinstance(result, CursorResult)
        assert result.rowcount == data.shape[0]
        self.__dbconnobj.delete_table(table_name=table_name)
        self.__dbconnobj.close_connection()
        os.remove("data.csv")
