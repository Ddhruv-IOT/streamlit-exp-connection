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
        self.client = client

        return client[database][collection_name]

    def show_all_documents(self, ttl: int = 1000, **kwargs):
        @cache_data(ttl=ttl)
        def _find_all_documents():
            return pd.DataFrame(list(self._instance.find(**kwargs)))

        return _find_all_documents()

    def find(self, filter: dict = None, ttl: int = 1000, **kwargs) -> pd.DataFrame:
        @cache_data(ttl=ttl)
        def _find(
            filter: dict = None,
        ):
            query = filter or {}

            # Extract any additional query options from **kwargs
            projection = kwargs.get("projection")
            sort = kwargs.get("sort")
            limit = kwargs.get("limit")
            skip = kwargs.get("skip")

            # Perform the find operation with additional query options
            cursor = self._instance.find(
                query, projection=projection, sort=sort, limit=limit, skip=skip
            )

            return pd.DataFrame(list(cursor))

        return _find(filter)

    def find_one(self, filter: dict = None, ttl: int = 1000, **kwargs) -> pd.Series:
        def _find_one(filter: dict = None):
            query = filter or {}

            # Perform the find_one operation with additional query options
            result = self._instance.find_one(query, projection=kwargs.get("projection"))

            return pd.Series(result) if result else pd.Series()

        return _find_one(filter)

    def insert_document(self, document: dict, **kwargs):
        return self._instance.insert_one(document, **kwargs)

    def insert_many_documents(self, documents: list, **kwargs):
        return self._instance.insert_many(documents, **kwargs)

    def update_document(self, query: dict, update: dict, **kwargs):
        return self._instance.update_one(query, {"$set": update}, **kwargs)

    def update_documents(self, query: dict, update: dict, **kwargs):
        return self._instance.update_many(query, {"$set": update}, **kwargs)

    def delete_document(self, query: dict, **kwargs):
        return self._instance.delete_one(query, **kwargs)

    def delete_documents(self, query: dict, **kwargs):
        return self._instance.delete_many(query, **kwargs)

    def count_documents(self, query: dict, ttl: int = 1000, **kwargs):
        @cache_data(ttl=ttl)
        def _count_documents(query: dict, **kwargs):
            return self._instance.count_documents(query, **kwargs)

        return _count_documents(query, **kwargs)

    def distinct_values(
        self, field: str, query: dict = None, ttl: int = 1000, **kwargs
    ):
        @cache_data(ttl=ttl)
        def _distinct_values(field: str, query: dict = None, **kwargs):
            return self._instance.distinct(field, filter=query, **kwargs)

        return _distinct_values(field, query, **kwargs)

    def query(self, query: dict, ttl: int = 3600, **kwargs) -> pd.DataFrame:
        @cache_data(ttl=ttl)
        def _query(query: dict, **kwargs) -> pd.DataFrame:
            return pd.DataFrame(list(self._instance.find(query, **kwargs)))

        return _query(query, **kwargs)

    def paginate_documents(
        self, page_number: int, items_per_page: int, ttl: int = 1000
    ) -> pd.DataFrame:
        @cache_data(ttl=ttl)
        def _paginate_documents(page_number: int, items_per_page: int) -> pd.DataFrame:
            # Calculate the number of documents to skip based on the page number and items per page
            skip_count = (page_number - 1) * items_per_page

            # Perform the query with pagination
            documents = self._instance.find().skip(skip_count).limit(items_per_page)
            return pd.DataFrame(list(documents))

        return _paginate_documents(page_number, items_per_page)
    
    def close(self):
        self.client.close()
        return "Connection closed"
