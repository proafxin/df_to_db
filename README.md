# Dataframe to Database

Write a pandas dataframe to a database directly. The `df.to_sql` is severely insufficient for this purpose. It not only overwrites the current table, it also requires manually creating an `SQLAlchemy` engine for connection. `dataframe-to-database` is meant to take all the extra steps away from this writing process. Currently, the goal is to support both SQL and NoSQL databases including data warehouse such as `Google BigQuery` or `Apache Cassandra`. For SQL databases, `SQLAlchemy` is used internally for generalizing all SQL database connections.


# Notes for Linux

You may need some packages otherwise `mysqlclient` installation may fail. Command for installing these in Debian/Ubuntu: `sudo apt-get install python3-dev default-libmysqlclient-dev build-essential`.