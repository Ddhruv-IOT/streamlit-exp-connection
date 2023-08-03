# import pwd_
import streamlit as st
from mongodb_conn import MongoDBConnection
from streamlit_option_menu import option_menu

# sidebar menu
with st.sidebar:
    selected = option_menu(
        "Db Operations",
        ["Connect", "Read", "Write", "Update", "Delete", "Find", "query", "Extra"],
        icons=["plug", "book", "pencil", "pen", "trash", "search", "question", "star"],
        menu_icon="database",
        default_index=0,
    )
    selected

# connect to mongodb using st.experimental_connection
conn = st.experimental_connection("mongodb", type=MongoDBConnection)

if selected == "Connect":
    st.write(conn)

elif selected == "Read":
    st.header("Showing All Documents")

    # fetch all documents from the collection
    data = conn.show_all_documents(ttl=1000)

    # display the data
    st.dataframe(data, width=1000)

    st.divider()

    st.header("Reading with Pagination")

    # Values to use for pagination
    page_number = st.slider("Page Number", 1, 10, 3)
    number_of_docs = st.slider("Number of Docs", 1, 10, 5)

    # perform the pagination and display the data
    st.dataframe(
        conn.paginate_documents(
            page_number=page_number,
            items_per_page=number_of_docs,
            ttl=1000,
        ),
        width=1000,
    )

elif selected == "Write":
    st.header("Insert a Single Document")
    data = st.text_input("Write a Test Name")
    if data in "":
        st.warning("Please enter a name")
    else:
        st.write("You have Entered: ", data)
        with st.spinner("Writing to Database"):
            op = conn.insert_document({"name": data})
            st.write(op)

        if st.button("Show All Documents after Update"):
            data = conn.show_all_documents(ttl=0)
            st.dataframe(data)

    st.divider()

    # Insert many documents into the collection
    documents_to_insert = [
        {"name": "Alice", "age": 25},
        {"name": "Bob", "age": 30},
        {"name": "Carol", "age": 35},
    ]
    result = conn.insert_many_documents(documents_to_insert)
    st.write("Inserted document IDs:", result.inserted_ids)

# z = conn.find_all_documents(ttl=0)
# st.dataframe(z)

# # Update a document in the collection
# query = {"name": "Alice"}
# update = {"age": 26}
# result = conn.update_document(  query, update)
# st.write("Update result:", result.modified_count)

# # Update multiple documents in the collection
# query = {"age": {"$lt": 30}}
# update = {"status": "young"}
# result = conn.update_documents(  query, update)
# st.write("Update result:", result.modified_count)

# # Delete a document from the collection
# query = {"name": "Carol"}
# result = conn.delete_document(  query)
# st.write("Deletion count:", result.deleted_count)

# # Delete multiple documents from the collection
# query = {"age": {"$gte": 26}}
# result = conn.delete_documents(  query)
# st.write("Deletion count:", result.deleted_count)

# # Count documents in the collection based on a query
# query = {"status": "young"}
# count = conn.count_documents(  query)
# st.write("Number of young people:", count)

# # Get distinct values of a field in the collection
# field_name = "name"
# distinct_values = conn.distinct_values(  field_name)
# st.write("Distinct names:", distinct_values)

# # Don't forget to close the connection when you're done
# # conn.close()
# # Please replace <connection-string> with your actual M

# z = conn.find_all_documents(ttl=0)
# st.dataframe(z)


# filter = {"name": "Abc"}
# projection = {"name": 1, }
# result   = conn.find_one( filter=filter, projection=projection)
# st.write(result)
# import pymongo

# filter = {"age": {"$gte": 25}}
# projection = {"name": 1, "age": 1}
# sort = [("age", pymongo.ASCENDING)]
# limit = 5
# skip = 0
# result2 = conn.find( filter=filter, projection=projection, sort=sort, limit=limit, skip=skip)

# st.write(result2)

# st.write(conn.close())
