![Build](https://github.com/proafxin/df_to_db/actions/workflows/build.yml/badge.svg)
![codecov](https://github.com/proafxin/df_to_db/blob/develop/coverage.svg)

# Dataframe to Database

Write a pandas dataframe to a database directly. The `df.to_sql` is severely insufficient for this purpose. It not only overwrites the current table, it also requires manually creating an `SQLAlchemy` engine for connection. `dataframe-to-database` is meant to take all the extra steps away from this writing process. Currently, the goal is to support both SQL and NoSQL databases including data warehouse such as `Google BigQuery` or `Apache Cassandra`. For SQL databases, `SQLAlchemy` is used internally for generalizing all SQL database connections.

## Supported So Far
* MySQL
* Postgresql
* SQL Server
* Mongo


# Notes for Linux

You may need some packages otherwise `mysqlclient` installation may fail. Command for installing these in Debian/Ubuntu: `sudo apt-get install python3-dev default-libmysqlclient-dev build-essential`.


# Environment setup

Create a virtual environment and activate it. Inside the virtual environment, run `pip install -r requirements.txt`. If you get this error `git clone --filter=blob:none --quiet 'ssh://****@github.com/proafxin/df_to_db.git' '..\df_to_db\env\src\dataframe-to-database' did not run successfully`, then go to `requirements.txt` and remove the line `-e git+ssh://git@github.com/proafxin/df_to_db.git...`. Now the command should run succesfully. If you are on windows and get the following error `tests run-test: commands[0] | python3 calculate_coverage.py WARNING: test command found but not installed in testenv`, then replace `python3 calculate_coverage.py` by `python calculate_coverage.py` or make `python3` available in tox.

# Example Usage

## Writing to SQL Database

Create a writer object. For example, if you want to create a writer object for SQL databases,

    from write_df.sql_writer import SQLDatabaseWriter

    writer = SQLDatabaseWriter(
        dbtype="mysql",
        host=MYSQL_HOST,
        dbname=DBNAME,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        port=MYSQL_PORT,
    )

Get the list of databases using the connection.

    database_names = writer.get_list_of_database()

Check if the database has a certain table `table_name`.

    writer.has_table(table_name=table_name)

Write the dataframe to the database using this connection.

    result = writer.write_df_to_db(
        data=data,
        table_name=table_name,
        id_col=None,
        drop_first=True,
    )

This `result` is an SQLAlchemy `CursorResult` object. `id_col` is the column name of the primary key (corresponding to `id` column of a table). If this column exists in the dataframe itself, pass the name of the column in this argument.


## Generate Documentation


Run `sphinx-quickstart`. Choose `y` when it asks to seperate build and source directories.

Change to `docs/source` directory. In `conf.py`, add the following lines at the start of the script. 

    
    import os
    import sys
    sys.path.insert(0, os.path.abspath("../.."))
    
and save it. Add `"sphinx.ext.autodoc",` to the `extensions` list. Run `python -m pip install -U sphinx_rtd_theme` and set `html_theme = "sphinx_rtd_theme"`.

In `index.rst`, add `modules` to toctree. The structure should look like this:

    
    .. toctree::
    :maxdepth: 2
    :caption: Contents:

    modules
    

Run `sphinx-apidoc -f -o . ../../ ../../calculate_coverage.py  ../../setup.py ../../tests/`. It should generate the necessary ReStructuredText files for documentation.

## Generating output
Change to `docs/` using `cd ..` then run `.\make clean` and `.\make html`. Output should be built with no errors or warnings. You will get the html documenation in `docs/build/html` directory. Open `index.html`.
