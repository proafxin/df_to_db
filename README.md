# Dataframe to Database

Write a pandas dataframe to a database directly. The `to_sql` is insufficient for this purpose. For example, it will create a new table or replace the existing one. Also, it requires manually creating an `SQLAlchemy` engine every time. This is a wrapper meant to remove that extra layer.
