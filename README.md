![Build](https://github.com/proafxin/df_to_db/actions/workflows/build.yml/badge.svg)
![codecov](https://github.com/proafxin/df_to_db/blob/develop/coverage.svg)

# Dataframe to Database

Write a pandas dataframe to a database directly. The `df.to_sql` is severely insufficient for this purpose. It not only overwrites the current table, it also requires manually creating an `SQLAlchemy` engine for connection. `dataframe-to-database` is meant to take all the extra steps away from this writing process. Currently, the goal is to support both SQL and NoSQL databases including data warehouse such as `Google BigQuery` or `Apache Cassandra`. For SQL databases, `SQLAlchemy` is used internally for generalizing all SQL database connections.


# Notes for Linux

You may need some packages otherwise `mysqlclient` installation may fail. Command for installing these in Debian/Ubuntu: `sudo apt-get install python3-dev default-libmysqlclient-dev build-essential`.


# Environment setup

Create a virtual environment and activate it. Inside the virtual environment, run `pip install -r requirements.txt`. If you get this error `git clone --filter=blob:none --quiet 'ssh://****@github.com/proafxin/df_to_db.git' 'D:\Projects\df_to_db\env\src\dataframe-to-database' did not run successfully`, then go to `requirements.txt` and remove the line `-e git+ssh://git@github.com/proafxin/df_to_db.git...`. Now the command should run succesfully. If you are on windows and get the following error `tests run-test: commands[0] | python3 calculate_coverage.py WARNING: test command found but not installed in testenv`, then replace `python3 calculate_coverage.py` by `python calculate_coverage.py` or make `python3` available in tox.