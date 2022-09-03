"""Test dataframe to database module"""

import os

# import pymysql as pms
from sqlalchemy.future import Connection, Engine
from sqlalchemy.orm import Session
from write_df.df_to_db import _get_column_info, _get_sql_alchemy_engine

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

        # for table_name in TABLE_NAMES:
        #     print(table_name)
        #     info = _get_column_info(cursor=cursor, table_name=table_name.strip(), dbname=DBNAME)
        #     print(info.head())
        #     print(info["COLUMN_NAME"].to_numpy())
        #     print(info["IS_NULLABLE"].to_numpy())
