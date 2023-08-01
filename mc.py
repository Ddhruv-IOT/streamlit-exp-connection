from streamlit.connections import ExperimentalBaseConnection
from streamlit.runtime.caching import cache_data
import pymongo
import pandas as pd

class MongoDBConnection(ExperimentalBaseConnection[pymongo.MongoClient]):
    """Basic st.experimental_connection implementation for MongoDB using pymongo"""

    def _connect(self, **kwargs) -> pymongo.MongoClient:
        if 'connection_string' in kwargs:
            connection_string = kwargs.pop('connection_string')
        else:
            connection_string = self._secrets['connection_string']
        return pymongo.MongoClient(connection_string, **kwargs)
    
    def get_collection(self, database_name: str, collection_name: str):
        return self._instance[database_name][collection_name]

    def find_all_documents(self, database_name: str, collection_name: str):
        collection = self.get_collection(database_name, collection_name)
        return pd.DataFrame(list(collection.find()))

    def insert_document(self, database_name: str, collection_name: str, document: dict):
        collection = self.get_collection(database_name, collection_name)
        return collection.insert_one(document)

    def query(self, database_name: str, collection_name: str, query: dict, ttl: int = 3600) -> pd.DataFrame:
        @cache_data(ttl=ttl)
        def _query(database_name: str, collection_name: str, query: dict) -> pd.DataFrame:
            collection = self.get_collection(database_name, collection_name)
            return pd.DataFrame(list(collection.find(query)))
        
        return _query(database_name, collection_name, query)

# Replace <connection-string> with your MongoDB Atlas connection string
connection_string = ""

# from pymongo import MongoClient
import streamlit as st
conn = st.experimental_connection("mongodb", type=MongoDBConnection, connection_string=connection_string)
# x = conn.get_collection("student_database", "students")
# st.write(x)
z = conn.find_all_documents("student_database", "students")
# st.dataframe(z)
st.write(z)

# for s in z:
#     st.write(s)
# st.write(z)