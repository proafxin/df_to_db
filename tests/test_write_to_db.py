"""Test dataframe to database module"""

import os

import pymysql as pms
from write_df.df_to_db import _get_column_info, _get_mysql_connection

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

        conn = _get_mysql_connection(
            host=HOST,
            dbname=DBNAME,
            user=USER,
            password=PASSWORD,
            port=PORT,
        )
        assert isinstance(conn, pms.connect)
        for table_name in TABLE_NAMES:
            print(table_name)
            info = _get_column_info(
                cursor=conn.cursor(), table_name=table_name.strip(), dbname=DBNAME
            )
            print(info.head())
            print(info["COLUMN_NAME"].to_numpy())
            print(info["IS_NULLABLE"].to_numpy())
        assert 1 == 2
