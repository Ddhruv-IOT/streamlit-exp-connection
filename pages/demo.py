# import pwd_
import streamlit as st
from mongodb_conn import MongoDBConnection
from streamlit_option_menu import option_menu

# Fake things ahead !!!
from faker import Faker  # for generating fake user names
import random as rd  # for generating random user age

# Create a Faker instance
fake = Faker()


# sidebar menu
with st.sidebar:
    selected = option_menu(
        "Db Operations",
        ["Connect", "Read", "Write", "Update", "Delete", "query", "Extra"],
        icons=["plug", "book", "pencil", "pen", "trash", "question", "star"],
        menu_icon="database",
        default_index=0,
    )
    st.info(f"Currently on {selected} page")

# connect to mongodb using st.experimental_connection
conn = st.experimental_connection("mongodb", type=MongoDBConnection)

if selected == "Connect":
    st.header("Connection Details")
    st.write(conn)
    st.help(conn)

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

    st.info(
        "Session state has been used to keep track of the insertion and will allow insertion only once",
        icon="⚠️",
    )
    if "written" not in st.session_state:
        st.session_state["written"] = False

    # if no data is entered, so it should not be inserted
    if data in "":
        st.warning("Please enter a name")
    else:
        st.write("You have Entered: ", data)
        st.write("Write Chance Left: ", 1 - st.session_state["written"])

        # display loading spinner while data is being inserted
        with st.spinner("Writing to Database"):
            # insert the data into the collection
            if st.session_state["written"] == False:
                op = conn.insert_document({"name": data, "age": 20})
                st.write(op)

            st.session_state["written"] = True

        if st.button("Show All Documents after Update", key="show_all"):
            # kept ttl=0 so that it will not be cached and updated data will be shown
            data = conn.show_all_documents(ttl=0)
            st.dataframe(data, width=1000)

    st.divider()

    # insert many documents into the collection
    st.header("Insert Many Documents")

    # generate fake data on the fly
    def fake_data():
        documents_to_insert = [
            {"name": fake.name(), "age": rd.randint(20, 40)},
            {"name": fake.name(), "age": rd.randint(20, 80)},
            {"name": fake.name(), "age": rd.randint(40, 90)},
        ]
        return documents_to_insert

    if st.button("Generate Fake Data & Insert"):
        documents_to_insert = fake_data()
        st.write("Documents to insert:")
        st.code(documents_to_insert, language="python")

        # insert the documents into the collection and get the inserted IDs
        result = conn.insert_many_documents(documents_to_insert)
        st.write("Inserted document IDs:", result.inserted_ids)

        st.write("Showing All Documents after Insertion")
        # kept ttl=0 so that it will not be cached and updated data will be shown
        data = conn.show_all_documents(ttl=0)
        st.dataframe(data, width=1000)


if selected == "Update":
    st.header("Update a Single Document")

    # Update a document in the collection
    with st.form(key="update"):
        name = st.text_input("Enter Name")
        query = {"name": name}
        select_age = st.slider("Select Age", 1, 100, 3)
        update = {"age": select_age}
        submit = st.form_submit_button("Update")

    st.info("Only the first matching document will be updated!", icon="❗")

    # update the document in the collection, only the first matching document will be updated
    result = conn.update_document(query, update)

    # display the result, if 0 it means no document was updated
    st.write("Update result:", result.modified_count)

    # display the updated data
    data = conn.show_all_documents(ttl=0)
    st.dataframe(data, width=1000)

    st.divider()
    st.header("Update Multiple Documents")

    with st.form(key="update_multiple"):
        select_age_limit = st.slider("Select age limit for status young", 1, 55, 3)
        submit = st.form_submit_button("Update")

    query = {"age": {"$lt": select_age_limit}}
    update = {"status": "young"}
    result = conn.update_documents(query, update)
    st.write("Update result for young", result.modified_count)

    query = {"age": {"$gt": select_age_limit}}
    update = {"status": "old"}
    result = conn.update_documents(query, update)
    st.write("Update result for old:", result.modified_count)

    # display the updated data
    data = conn.show_all_documents(ttl=0)
    st.dataframe(data, width=1000)


if selected == "Delete":
    st.header("Delete a Single Document")

    # Delete a document in the collection
    with st.form(key="delete"):
        name = st.text_input("Enter Name to Delete")
        query_delete = {"name": name}
        submit = st.form_submit_button("Delete")

    st.info("Only the first matching document will be deleted!", icon="❗")

    # Delete a document from the collection
    st.write("Query to delete:", query_delete)
    result = conn.delete_document(query_delete)
    st.write("Deletion count: ", result.deleted_count)
    
    # display the updated data
    data = conn.show_all_documents(ttl=0)
    st.dataframe(data, width=1000)

    st.divider()

    st.header("Delete Multiple Documents")

    st.write(
        "The code for deletion of multiple documents is commented out for safety reasons"
    )
    code = """ 
            # Delete multiple documents from the collection
            
            # The query or the condition to delete multiple documents
            query = {"age": {"$gte": 26}}
            
            # the class method to delete multiple documents 
            result = conn.delete_documents(query) 
            
            # Display the number of documents deleted, 0 means no document was deleted
            st.write("Deletion count:", result.deleted_count) 
            """
    st.code(code, language="python")


if selected == "query":
    query_filter = st.text_input("Enter your Mongo Query")
    # query_filter = {"age": {"$gte": 25}, }
    try:
        query_filter = eval(query_filter)
        st.success("Running query: " + str(query_filter))
    except Exception as e:
        st.error(e)
        query_filter = {}
        st.info("Running default query: All")

    result = conn.query(query=query_filter, ttl=1000)
    st.write(result)

if selected == "Extra":
    st.header("Extra Features")

    st.write("Example usage of extra features are shown below")

    st.subheader("Count Documents")
    # Count documents in the collection based on a query
    st.write("The code")
    st.code(
        """
            query = {"status": "young"}
            count = conn.count_documents(query)
            st.write("Number of young people:", count)
            """
    )

    query = {"status": "young"}
    count = conn.count_documents(query)
    st.write("Number of young people:", count)

    st.subheader("Distinct Values")
    # Get distinct values of a field in the collection
    st.write("The code")
    st.code(
        """
            field_name = "name"
            distinct_values = conn.distinct_values(field_name)
            st.write("Distinct names:", distinct_values)

            """
    )
    field_name = "name"
    distinct_values = conn.distinct_values(field_name)
    st.info("Displaying only first 10 names")
    st.write("Distinct names:", distinct_values[0:10])
    
    st.error("Find and Find all functions haven't been covered in this demo. Please refer to the documentation for more details")
