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
        
        if "collection_name" in kwargs:
            collection_name = kwargs.pop("collection_name")
        else:
            collection_name = self._secrets["collection_name"]


        client = pymongo.MongoClient(connection_string, **kwargs)

        return client[database][collection_name]

    def find_all_documents(self, ttl: int = 1000):
        @cache_data(ttl=ttl)
        def _find_all_documents():
            return pd.DataFrame(list(self._instance.find()))
        return _find_all_documents()

    def insert_document(self, document: dict):
        return self._instance.insert_one(document)

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
