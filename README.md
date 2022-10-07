![Build](https://github.com/proafxin/df_to_db/actions/workflows/build.yml/badge.svg)
![codecov](https://github.com/proafxin/df_to_db/blob/develop/coverage.svg)

# Dataframe to Database

Write a pandas dataframe to a database directly. The `df.to_sql` is severely insufficient for this purpose. It not only overwrites the current table, it also requires manually creating an `SQLAlchemy` engine for connection. `df-to-db` is meant to take all the extra steps away from this writing process. Currently, the goal is to support both SQL and NoSQL databases including data warehouse such as `Google BigQuery` or `Apache Cassandra`. For SQL databases, `SQLAlchemy` is used internally for generalizing all SQL database connections.

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

For SQL databases, use `SQLDatabaseWriter`.

```python
from write_df.sql_writer import SQLDatabaseWriter

writer = SQLDatabaseWriter(
    dbtype="mysql",
    host=MYSQL_HOST,
    dbname=DBNAME,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    port=MYSQL_PORT,
)
```

`dbtype` can be one of the SQL databses supported i.e. one of `mysql, postgresql, sqlserver`.
Get the list of databases using the connection.

```python
database_names = writer.get_list_of_database()
```

Check if the database has a certain table `table_name`.

```python
writer.has_table(table_name=table_name)
```

Write the dataframe to the database using this connection.

```python
result = writer.write_df_to_db(
    data=data,
    table_name=table_name,
    id_col=None,
    drop_first=True,
)
```

`data` is the actual dataframe to write. This `result` is an SQLAlchemy `CursorResult` object. `id_col` is the column name of the primary key (corresponding to `id` column of a table). If this column exists in the dataframe itself, pass the name of the column in this argument. If `drop_first` is `True`, then the table will be dropped and created from the dataframe schema. Otherwise, the writer will read the schema from the database, check whether there is any null data in non-nullable columns and then try to write the data to the table. Needless to say, the column names must be identical in dataframe and the table.


## Writing to NoSQL Database

Create the writer object.

```python
from write_df.nosql_writer import NoSQLDatabaseWriter

writer = NoSQLDatabaseWriter(
    dbtype="mongo",
    host=os.environ["MONGO_HOST"],
    dbname=DBNAME,
    user=os.environ["MONGO_USER"],
    password=os.environ["MONGO_PASSWORD"],
    port=int(os.environ["MONGO_PORT"]),
)
```

Get list of databases.

```python
dbnames = writer.get_list_of_databases()
```

Get list of collections of the current database.

```python
collection_names = writer.get_list_of_collections()
```

Get a certain collection or create it.

```python
collection = writer.get_or_create_collection(collection_name=collection_name)
```

Get document count of a collection.

```python
doc_count = writer.get_document_count(collection_name=collection_name)
```

Write to a collection `collection_name`.

```python
result = write_data_to_collection(collection_name=collection_name, data=data)
```



## Generate Documentation


Run `sphinx-quickstart`. Choose `y` when it asks to seperate build and source directories.

Change to `docs/source` directory. In `conf.py`, add the following lines at the start of the script. 

```python
import os
import sys

sys.path.insert(0, os.path.abspath("../.."))
```

and save it. Add `"sphinx.ext.autodoc",` to the `extensions` list. Run `python -m pip install -U sphinx_rtd_theme` and set `html_theme = "sphinx_rtd_theme"`.

In `index.rst`, add `modules` to toctree. The structure should look like this:

```
.. toctree::
:maxdepth: 2
:caption: Contents:

modules
```

Run `sphinx-apidoc -f -o . ../../ ../../calculate_coverage.py  ../../setup.py ../../tests/`. It should generate the necessary ReStructuredText files for documentation.

## Generating output
Change to `docs/` using `cd ..` then run `.\make clean` and `.\make html`. Output should be built with no errors or warnings. You will get the html documenation in `docs/build/html` directory. Open `index.html`.
