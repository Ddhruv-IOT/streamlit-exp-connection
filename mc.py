from streamlit.connections import ExperimentalBaseConnection
from streamlit.runtime.caching import cache_data
import pymongo
import pandas as pd
from pwd_ import pw


class MongoDBConnection(ExperimentalBaseConnection[pymongo.MongoClient]):
    """Basic st.experimental_connection implementation for MongoDB using pymongo"""

    def _connect(self, **kwargs) -> pymongo.MongoClient:
        if "connection_string" in kwargs:
            connection_string = kwargs.pop("connection_string")
        else:
            connection_string = self._secrets["connection_string"]

        if "database" in kwargs:
            database = kwargs.pop("database")
        else:
            database = self._secrets["database"]

        client = pymongo.MongoClient(connection_string, **kwargs)

        return client[database]

    def get_collection(self, collection_name: str) -> pymongo.collection.Collection:
        self.collection = collection_name
        return self._instance[collection_name]

    def find_all_documents(self, collection_name: str = None):
        collection = self.get_collection(self.collection)
        if collection_name:
            collection = self.get_collection(collection_name)
        return pd.DataFrame(list(collection.find()))

    def insert_document(self, collection_name: str, document: dict):
        collection = self.get_collection(collection_name)
        return collection.insert_one(document)

    def query(
        self, database_name: str, collection_name: str, query: dict, ttl: int = 3600
    ) -> pd.DataFrame:
        @cache_data(ttl=ttl)
        def _query(
            database_name: str, collection_name: str, query: dict
        ) -> pd.DataFrame:
            collection = self.get_collection(database_name, collection_name)
            return pd.DataFrame(list(collection.find(query)))

        return _query(database_name, collection_name, query)
