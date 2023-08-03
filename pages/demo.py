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
                op = conn.insert_document({"name": data})
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
    result = conn.update_documents( query, update)
    st.write("Update result for young", result.modified_count)
    
    query = {"age": {"$gt": select_age_limit}}
    update = {"status": "old"}
    result = conn.update_documents( query, update)
    st.write("Update resul for old:", result.modified_count)
    
     # display the updated data
    data = conn.show_all_documents(ttl=0)
    st.dataframe(data, width=1000)



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
