import streamlit as st

st.set_page_config(page_title="Home Page", page_icon="🏡")

from streamlit_option_menu import option_menu

selected2 = option_menu(
    None,
    ["Home", "Set-Up", "Related Links"],
    icons=["house", "gear", "list-task", "gear"],
    menu_icon="cast",
    default_index=0,
    orientation="horizontal",
)

if selected2 == "Home":
    data = \
    """
        <h3>Streamlit Connections Hackathon: Building Connections for MongoDB</h3>

        <p>Are you tired of the hassle and complexity of setting up data connections for your Streamlit apps? Look no further! The Streamlit Connections Hackathon is here to empower developers like you with the tools to connect your apps seamlessly to various data sources. We are excited to announce that our recent release of <code>st.experimental_connection</code> has made data integration easier than ever before!</p>

        <p>The Streamlit Connections Hackathon aims to explore the vast possibilities of <code>st.experimental_connection</code> and to encourage developers to build new connections for different data sources and APIs. The focus of this write-up will be on developing a connection for MongoDB, a popular NoSQL database.</p>

        <h4>MongoDB Connection: Empowering Streamlit Apps</h4>
        <p>MongoDB is a powerful and flexible NoSQL database that allows for the storage of unstructured data in JSON-like documents. It's widely used in modern web applications and data-driven projects due to its scalability and ease of use. By developing a MongoDB connection for Streamlit, we enable developers to seamlessly integrate MongoDB into their apps, harnessing its capabilities for data storage and retrieval.</p>
    """
    
    st.write(data, unsafe_allow_html=True)

if selected2 == "Set-Up": 
    data = \
    """
        <h3>Getting Started: Building the MongoDB Connection Class</h3>
        <div> 
            <p><strong>Install MongoDB Python Driver:</strong> Before implementing the connection logic, ensure you have the MongoDB Python driver installed. This package will facilitate communication with your MongoDB instance.</p>
        </div>
   
        <pre>
            <code class="language-bash">
                pip install pymongo
            </code>
        </pre>
        
        <br/>
        Then, import the necessary packages:
        
        
        <pre>
            <code class="language-python">
                from mongodb_conn import MongoDBConnection
            </code>
        </pre>
        
        Now, instantate the MongoDBConnection class:
        <pre> <code>
                connector = st.experimental_connection(
            "mongodb",
            type=MongoDBConnection,
            
            # can specify connection_string
            # database,
            # and collection_name here or in secrets.toml)
        </code> </pre>
    
        <b> To keep data in secrets.toml, add the following lines: </b>
        <pre> 
        <code>
        [connections.mongodb]
        connection_string = "mongodb+srv://name:password@cluster0.zsfxajf.mongodb.net/?retryWrites=true&w=majority"
        database = "student_database"
        collection_name = "student"
        </code> 
        </pre>  
        
    """
    
    st.write(data, unsafe_allow_html=True)

if selected2 == "Related Links":
    data = \
    """
        <h3>Source Code 👨‍💻</h3>
        
    """
    
    st.write(data, unsafe_allow_html=True)
    st.write("Get the code [here](https://github.com/Ddhruv-IOT/streamlit-exp-connection) ")
    st.write("<h2>Thank You!</h2>", unsafe_allow_html=True)
    

# db_demo()
