from streamlit.connections import ExperimentalBaseConnection
from streamlit.runtime.caching import cache_data
import pymongo
import pandas as pd


class MongoDBConnection(ExperimentalBaseConnection[pymongo.MongoClient]):
    """Basic st.experimental_connection implementation for MongoDB using pymongo"""

    def _connect(self, **kwargs) -> pymongo.MongoClient:
        """
        Connect to the MongoDB database using the given connection parameters.

        Parameters:
        - connection_string (str): The MongoDB connection string.
        - database (str): The name of the database to connect to.
        - collection_name (str): The name of the collection to connect to.
        - **kwargs: Additional keyword arguments to pass to pymongo.MongoClient.

        Returns:
        pymongo.MongoClient: The MongoDB client connected to the specified database and collection.
        """

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
        """
        Retrieve all documents from the MongoDB collection.

        Parameters:
        - ttl (int): Time-to-live for caching the result, in seconds.
        - **kwargs: Additional keyword arguments to pass to pymongo.Collection.find.

        Returns:
        pd.DataFrame: A DataFrame containing all the documents from the collection.
        """

        @cache_data(ttl=ttl)
        def _find_all_documents():
            return pd.DataFrame(list(self._instance.find(**kwargs)))

        return _find_all_documents()

    def find(self, filter: dict = None, ttl: int = 1000, **kwargs) -> pd.DataFrame:
        """
        Find documents in the MongoDB collection that match the specified filter.

        Parameters:
        - filter (dict): The filter to apply on the documents (default: None).
        - ttl (int): Time-to-live for caching the result, in seconds.
        - **kwargs: Additional keyword arguments to pass to pymongo.Collection.find.

        Returns:
        pd.DataFrame: A DataFrame containing the documents that match the filter.
        """

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
        """
        Find a single document in the MongoDB collection that matches the specified filter.

        Parameters:
        - filter (dict): The filter to apply on the documents (default: None).
        - ttl (int): Time-to-live for caching the result, in seconds.
        - **kwargs: Additional keyword arguments to pass to pymongo.Collection.find_one.

        Returns:
        pd.Series: A pandas Series representing the matching document, or an empty Series if not found.
        """

        def _find_one(filter: dict = None):
            query = filter or {}

            # Perform the find_one operation with additional query options
            result = self._instance.find_one(query, projection=kwargs.get("projection"))

            return pd.Series(result) if result else pd.Series()

        return _find_one(filter)

    def insert_document(self, document: dict, **kwargs):
        """
        Insert a single document into the MongoDB collection.

        Parameters:
        - document (dict): The document to insert.
        - **kwargs: Additional keyword arguments to pass to pymongo.Collection.insert_one.
        """
        return self._instance.insert_one(document, **kwargs)

    def insert_many_documents(self, documents: list, **kwargs):
        """
        Insert multiple documents into the MongoDB collection.

        Parameters:
        - documents (list): A list of documents to insert.
        - **kwargs: Additional keyword arguments to pass to pymongo.Collection.insert_many.
        """
        return self._instance.insert_many(documents, **kwargs)

    def update_document(self, query: dict, update: dict, **kwargs):
        """
        Update a single document in the MongoDB collection that matches the specified query.

        Parameters:
        - query (dict): The query to find the document to update.
        - update (dict): The update operation to apply on the matching document.
        - **kwargs: Additional keyword arguments to pass to pymongo.Collection.update_one.
        """
        return self._instance.update_one(query, {"$set": update}, **kwargs)

    def update_documents(self, query: dict, update: dict, **kwargs):
        """
        Update multiple documents in the MongoDB collection that match the specified query.

        Parameters:
        - query (dict): The query to find the documents to update.
        - update (dict): The update operation to apply on the matching documents.
        - **kwargs: Additional keyword arguments to pass to pymongo.Collection.update_many.
        """
        return self._instance.update_many(query, {"$set": update}, **kwargs)

    def delete_document(self, query: dict, **kwargs):
        """
        Delete a single document from the MongoDB collection that matches the specified query.

        Parameters:
        - query (dict): The query to find the document to delete.
        - **kwargs: Additional keyword arguments to pass to pymongo.Collection.delete_one.
        """
        return self._instance.delete_one(query, **kwargs)

    def delete_documents(self, query: dict, **kwargs):
        """
        Delete multiple documents from the MongoDB collection that match the specified query.

        Parameters:
        - query (dict): The query to find the documents to delete.
        - **kwargs: Additional keyword arguments to pass to pymongo.Collection.delete_many.
        """
        return self._instance.delete_many(query, **kwargs)

    def count_documents(self, query: dict, ttl: int = 1000, **kwargs):
        """
        Count the number of documents in the MongoDB collection that match the specified query.

        Parameters:
        - query (dict): The query to count the documents.
        - ttl (int): Time-to-live for caching the result, in seconds.
        - **kwargs: Additional keyword arguments to pass to pymongo.Collection.count_documents.

        Returns:
        int: The number of documents that match the query.
        """

        @cache_data(ttl=ttl)
        def _count_documents(query: dict, **kwargs):
            return self._instance.count_documents(query, **kwargs)

        return _count_documents(query, **kwargs)

    def distinct_values(
        self, field: str, query: dict = None, ttl: int = 1000, **kwargs
    ):
        """
        Retrieve distinct values for a given field in the MongoDB collection.

        Parameters:
        - field (str): The field for which to retrieve distinct values.
        - query (dict): The query to filter the documents (default: None).
        - ttl (int): Time-to-live for caching the result, in seconds.
        - **kwargs: Additional keyword arguments to pass to pymongo.Collection.distinct.

        Returns:
        list: A list of distinct values for the specified field.
        """

        @cache_data(ttl=ttl)
        def _distinct_values(field: str, query: dict = None, **kwargs):
            return self._instance.distinct(field, filter=query, **kwargs)

        return _distinct_values(field, query, **kwargs)

    def query(self, query: dict, ttl: int = 3600, **kwargs) -> pd.DataFrame:
        """
        Execute a custom query on the MongoDB collection and retrieve the results as a DataFrame.

        Parameters:
        - query (dict): The custom query to execute.
        - ttl (int): Time-to-live for caching the result, in seconds.
        - **kwargs: Additional keyword arguments to pass to pymongo.Collection.find.

        Returns:
        pd.DataFrame: A DataFrame containing the results of the custom query.
        """

        @cache_data(ttl=ttl)
        def _query(query: dict, **kwargs) -> pd.DataFrame:
            return pd.DataFrame(list(self._instance.find(query, **kwargs)))

        return _query(query, **kwargs)

    def paginate_documents(
        self, page_number: int, items_per_page: int, ttl: int = 1000
    ) -> pd.DataFrame:
        """
        Paginate through the MongoDB collection and retrieve documents for the specified page.

        Parameters:
        - page_number (int): The page number to retrieve (1-based).
        - items_per_page (int): The number of items to retrieve per page.
        - ttl (int): Time-to-live for caching the result, in seconds.

        Returns:
        pd.DataFrame: A DataFrame containing the documents for the specified page.
        """

        @cache_data(ttl=ttl)
        def _paginate_documents(page_number: int, items_per_page: int) -> pd.DataFrame:
            # Calculate the number of documents to skip based on the page number and items per page
            skip_count = (page_number - 1) * items_per_page

            # Perform the query with pagination
            documents = self._instance.find().skip(skip_count).limit(items_per_page)
            return pd.DataFrame(list(documents))

        return _paginate_documents(page_number, items_per_page)

    # def close(self):
    #     self.client.close()
    #     return "Connection closed"
