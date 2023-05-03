# Import required libraries
import snowflake.connector
import streamlit as st
import pandas as pd

def write_to_snowflake(df, warehouse, database, schema, table):
  
    # establish a new connection to Snowflake
    conn = snowflake.connector.connect(
        user='rgriggs0072',
        password='Cyaamstr927!',
        account='OEZIERR-CNB82593',
        warehouse=warehouse,
        database='datasets',
        schema=schema
    )


    # read Excel file into pandas DataFrame
    df = pd.read_excel(uploaded_file)
    # replace NaN values with "NULL"
    df = df.where(pd.notnull(df), None)
  
     # write DataFrame to Snowflake
    cursor = conn.cursor()
    sql_query = "CREATE OR REPLACE TABLE tmp_table AS SELECT \
        CAST(STORE_NUMBER AS NUMERIC) AS STORE_NUMBER, \
        CAST(STORE_NAME AS VARCHAR) AS STORE_NAME, \
        CAST(UPC AS numeric) AS UPC, \
        CAST(IN_SCHEMATIC AS NUMBER) AS IN_SCHEMATIC, \
        CAST(PRODUCT_NAME AS VARCHAR) AS PRODUCT_NAME, \
        CAST(PURCHASED_YES_NO AS NUMBER) AS PURCHASED_YES_NO \
        FROM (VALUES {}) \
        AS tmp(STORE_NUMBER, STORE_NAME, UPC, IN_SCHEMATIC, PRODUCT_NAME, PURCHASED_YES_NO);".format(
            ', '.join([str(tuple(df.iloc[i].values)) for i in range(len(df))])
    )

    #st.write(sql_query)  # print the SQL query
    cursor.execute(sql_query)
    cursor.close()
    conn.close()

    st.write("Data has been imported into Snowflake!")


# create file uploader
uploaded_file = st.file_uploader("Upload Excel file", type=["xlsx"])

# check if file was uploaded
if uploaded_file:
    # read Excel file into pandas DataFrame
    df = pd.read_excel(uploaded_file)
    print(df.columns)
    # display DataFrame in Streamlit
    st.dataframe(df)

    # get warehouse and schema name from user
    warehouse_name = st.text_input("Enter warehouse name:")
    schema_name = st.text_input("Enter schema name:")
    print(df.columns)
    # write DataFrame to Snowflake on button click
    if st.button("Import into Snowflake"):
        write_to_snowflake(df, warehouse_name, "datasets", schema_name, "datasets")




    
 