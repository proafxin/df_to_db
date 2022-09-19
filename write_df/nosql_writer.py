"""Write a pandas dataframe to a NoSQL database collection"""


import pandas as pd
import pymongo
from write_df.common import nosql_dbtypes

__all__ = ["NoSQLDatabaseWriter"]


class MongoDatabaseWriter:
    """Writer class for Mongo databases"""

    def __init__(self, host: str, dbname: str, user: str, password: str, port: int) -> None:
        self.__client = self._get_mongo_client(
            host=host,
            username=user,
            password=password,
            port=port,
        )
        self.__dbname = dbname
        self.__db = self.__client[dbname]

    def _get_mongo_client(self, host: str, username: str, password: str, port: int):

        connection_string = (
            f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority"
        )

        client = pymongo.MongoClient(host=connection_string, port=port, document_class=dict)

        return client

    def get_list_of_databases(self):

        return self.__client.list_database_names()

    def get_list_of_collections(self):

        return self.__db.list_collection_names()

    def get_or_create_collection(self, collection_name: str):

        collection = self.__db[collection_name]

        return collection

    def write_data_to_collection(self, collection_name: str, data: pd.DataFrame):

        collection = self.get_or_create_collection(collection_name=collection_name)
        documents = data.to_dict("records")

        res = collection.insert_many(documents=documents)

        return res

    def get_document_count(self, collection_name: str):

        collection = self.get_or_create_collection(collection_name=collection_name)

        return collection.count_documents({})

    def delete_collection(self, collection_name: str):

        self.__db.drop_collection(collection_name)

    def delete_database(self):

        self.__client.drop_database(name_or_database=self.__dbname)

    def close_connection(self):

        self.__client.close()


class NoSQLDatabaseWriter:
    """Writer class for NoSQL Database"""

    def __init__(
        self, dbtype: str, host: str, dbname: str, user: str, password: str, port: int
    ) -> None:
        assert dbtype in nosql_dbtypes, f"{dbtype} not in {list(nosql_dbtypes.keys())}"
        self.__dbtype = dbtype

        self.__writer = self._get_writer(
            host=host, dbname=dbname, user=user, password=password, port=port
        )

    def _get_writer(self, host: str, dbname: str, user: str, password: str, port: int):

        if self.__dbtype == "mongo":
            return MongoDatabaseWriter(
                host=host, dbname=dbname, user=user, password=password, port=port
            )

        return None

    def get_list_of_databases(self):

        return self.__writer.get_list_of_databases()

    def get_list_of_collections(self):

        return self.__writer.get_list_of_collections()

    def get_or_create_collection(self, collection_name: str):

        return self.__writer.get_or_create_collection(collection_name=collection_name)

    def write_data_to_collection(self, collection_name: str, data: pd.DataFrame):

        return self.__writer.write_data_to_collection(collection_name=collection_name, data=data)

    def get_document_count(self, collection_name: str):

        return self.__writer.get_document_count(collection_name=collection_name)

    def delete_collection(self, collection_name: str):

        self.__writer.delete_collection(collection_name=collection_name)

    def delete_database(self):

        self.__writer.delete_database()

    def close_connection(self):

        self.__writer.close_connection()
