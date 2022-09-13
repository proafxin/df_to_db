"""Common variables for dataframe to database module"""


saved_values = {
    "mysql": {
        "dialect": "mysql",
        "driver": "mysqldb",
        "query": {
            "db_list": "SHOW DATABASES;",
            "table_list": "SHOW TABLES FROM ",
            "column_info": "select * from information_schema.columns WHERE table_schema='{}' and table_name='{}'",
        },
    },
    "postgresql": {
        "dialect": "postgresql",
        "driver": "psycopg2",
        "query": {
            "db_list": "select datname from pg_database;",
            "table_list": "select * from pg_catalog.pg_tables where schemaname=",
            "column_info": "select * from information_schema.columns WHERE table_catalog='{}' and table_name='{}'",
        },
    },
}
