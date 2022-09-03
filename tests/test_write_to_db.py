"""Test dataframe to database module"""

import os
from io import StringIO

import pandas as pd

# import pymysql as pms
from requests import get
from sqlalchemy.engine.cursor import CursorResult
from sqlalchemy.future import Engine
from sqlalchemy.orm import Session
from write_df.df_to_db import _delete_table, _get_sql_alchemy_engine, write_df_to_db

HOST = os.environ["MYSQL_HOST"]
USER = os.environ["MYSQL_USER"]
PASSWORD = os.environ["MYSQL_PASSWORD"]
PORT = int(os.environ["MYSQL_PORT"])
DBNAME = os.environ["MYSQL_DBNAME"]
TABLE_NAMES = os.environ["TEST_TABLE_NAMES"].split(",")


class TestWriteToMySQL:
    """Test class for writing to MySQL database"""

    def test_mysql_connection(self):
        """Test mysql connection from environment variables"""

        engine = _get_sql_alchemy_engine(
            dialect="mysql",
            host=HOST,
            user=USER,
            password=PASSWORD,
            dbname=DBNAME,
            port=PORT,
        )
        assert isinstance(engine, Engine), "engine is not correct"
        with Session(engine) as sess:
            assert isinstance(sess, Session)

        response = get(url="https://people.sc.fsu.edu/~jburkardt/data/csv/cities.csv")
        assert response.status_code == 200

        data = pd.read_csv(StringIO(response.content.decode()))
        table_name = "test__table__"
        result, table = write_df_to_db(
            data=data,
            host=HOST,
            dbname=DBNAME,
            user=USER,
            password=PASSWORD,
            port=PORT,
            table_name=table_name,
            primary_key="id",
            drop_first=True,
        )
        assert engine.dialect.has_table(connection=engine.connect(), table_name=table_name) is True
        assert isinstance(result, CursorResult)
        assert result.rowcount == data.shape[0]
        _delete_table(table=table, engine=engine)
