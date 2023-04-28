import streamlit as st
import snowflake.connector
import pandas as pd
import base64



# Set up Snowflake connection
conn = snowflake.connector.connect(
    user='MI',
    password='781256Mp**',
    account='LP15516.canada-central.azure',
    warehouse='COMPUTE_WH',
    database='SNOWFLAKE_SAMPLE_DATA',
    schema='TPCDS_SF100TCL'
)

# Define function to query Snowflake
def search_data(keyword):
    cursor = conn.cursor()
    query = f"SELECT S_STORE_SK, S_STORE_ID, S_REC_END_DATE, S_CLOSED_DATE_SK, S_STORE_NAME, S_STORE_NAME, S_REC_START_DATE FROM STORE WHERE S_STORE_NAME LIKE '%{keyword}%'"
    cursor.execute(query)
    results = cursor.fetchall()
    return results

# Set up Streamlit app
st.title("Search Data")

# Get search keyword from user
keyword = st.text_input("Enter keyword to search:")

# Display search results in a table and allow user to download as CSV
if keyword:
    results = search_data(keyword)
    if results:
        df = pd.DataFrame(results, columns=['1', '2', '3' ,'4', '5', 'Store Name', 'Sales'])

        st.write(df)
        csv = df.to_csv(index=False)
        b64 = base64.b64encode(csv.encode()).decode()
        href = f'<a href="data:file/csv;base64,{b64}" download="data.csv">Download CSV file</a>'
        st.markdown(href, unsafe_allow_html=True)
    else:
        st.write("No results found.")

# Allow user to insert data
st.subheader("Insert Data")
store_name = st.text_input("Enter store name:")
sales = st.number_input("Enter sales:")

# Define function to insert data into Snowflake
def insert_data(store_name, sales):
    cursor = conn.cursor()
    query = f"INSERT INTO STORE (S_STORE_NAME, S_SALES) VALUES ('{store_name}', {sales})"
    cursor.execute(query)
    conn.commit()
    
if st.button("Insert"):
    insert_data(store_name, sales)
    st.write("Data inserted successfully.")