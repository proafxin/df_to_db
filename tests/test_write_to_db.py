"""Test dataframe to database module"""

import os

import pymysql as pms
from write_df.df_to_db import _get_mysql_connection

HOST = os.environ["MYSQL_HOST"]
USER = os.environ["MYSQL_USER"]
PASSWORD = os.environ["MYSQL_PASSWORD"]
PORT = int(os.environ["MYSQL_PORT"])
DBNAME = os.environ["MYSQL_DBNAME"]


class TestWriteToMySQL:
    """Test class for writing to MySQL database"""

    def test_mysql_connection(self):
        """Test mysql connection from environment variables"""

        conn = _get_mysql_connection(
            host=HOST,
            dbname=DBNAME,
            user=USER,
            password=PASSWORD,
            port=PORT,
        )
        assert isinstance(conn, pms.connect)
