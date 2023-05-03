# Import required libraries
import snowflake.connector
import streamlit as st
import pandas as pd
import openpyxl
from openpyxl.utils import get_column_letter
from openpyxl import load_workbook
import numpy as np


# Displaying images on the front end
from PIL import Image
image = Image.open('images/DeltaPacific_Logo.jpg')
st.image(image, caption='',width=100)

st.title("Delta Pacific Beverage")
st.header("Gap Report and Analysis")




from io import BytesIO

def format_sales_report(workbook):
    # Delete all sheets except SALES_REPORT
    for sheet_name in workbook.sheetnames:
        if sheet_name != 'SALES_REPORT':
            workbook.remove(workbook[sheet_name])

    # Select the SALES_REPORT sheet
    ws = workbook['SALES_REPORT']

    # Delete row 2
    ws.delete_rows(2)

    # Delete column H
    ws.delete_cols(8)

    # Remove all hyphens from column F
    for cell in ws['F']:
        if cell.value is not None:
            cell.value = str(cell.value).replace('-', '')

    # Create a new column for store name
    ws.insert_cols(2)
    ws.cell(row=1, column=2, value='STORE NAME')

    # Copy values before the # to store name column
    for row in ws.iter_rows(min_row=2, min_col=3, max_col=3):
        for cell in row:
            if '#' in str(cell.value):
                cell_offset = ws.cell(row=cell.row, column=2)
                store_name = str(cell.value).split('#')[0].replace("'", "")
                cell_offset.value = store_name

    # Remove column C
    ws.delete_cols(3)

    # Replace all commas with spaces in column B
    for cell in ws['B']:
        if cell.value is not None and isinstance(cell.value, str):
            cell.value = cell.value.replace(',', ' ')

    # Remove all 's in column B
    for cell in ws['B']:
        if cell.value is not None and isinstance(cell.value, str):
            cell.value = cell.value.replace(" 's", "")

    # Replace all commas with spaces in column E
    for row in ws.iter_rows(min_row=2, min_col=5, max_col=5):
        for cell in row:
            if cell.value is not None and isinstance(cell.value, str):
                cell.value = cell.value.replace(',', ' ')

    # Format column G to number format with no decimals
    for row in ws.iter_rows(min_row=2, min_col=7, max_col=7):
        for cell in row:
            if isinstance(cell.value, str) and cell.value.strip() != '' and cell.value != 'Carrier UPC':
                cell.number_format = '0'
                cell.value = float(cell.value.replace(",", ""))

    return workbook


# Upload the workbook
uploaded_file = st.file_uploader("Upload freshly ran sales report from Encore", type=["xlsx", "xls"])

if uploaded_file is not None:
    # Load the workbook
    workbook = openpyxl.load_workbook(uploaded_file)

    # Show the Reformat button
    if st.button("Reformat"):
        # Format the sales report
        new_workbook = format_sales_report(workbook)

        # Download the formatted file
        new_filename = 'formatted_' + uploaded_file.name
        stream = BytesIO()
        new_workbook.save(stream)
        stream.seek(0)
        st.download_button(label="Download formatted file", data=stream.read(), file_name=new_filename, mime='application/vnd.ms-excel')



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
    df.fillna(value=np.nan, inplace=True)
    # write DataFrame to Snowflake
    cursor = conn.cursor()
    sql_query = "CREATE OR REPLACE TABLE tmp_table AS SELECT \
    CAST(STORE_NUMBER AS NUMBER) AS STORE_NUMBER, \
    CAST(STORE_NAME AS VARCHAR) AS STORE_NAME, \
    CAST(ADDRESS AS VARCHAR) AS ADDRESS, \
    CAST(SALESPERSON AS VARCHAR) AS SALESPERSON, \
    CAST(PRODUCT_NAME AS VARCHAR) AS PRODUCT_NAME, \
    CAST(UPC AS VARCHAR) AS UPC, \
    CAST(PURCHASED_YES_NO AS VARCHAR) AS PURCHASED_YES_NO \
    FROM (VALUES {}) \
    AS tmp(STORE_NUMBER, STORE_NAME, ADDRESS, SALESPERSON, PRODUCT_NAME, UPC, PURCHASED_YES_NO);".format(
        ', '.join([str(tuple(df.iloc[i].fillna(np.nan).values)) for i in range(len(df))])
    )
    #st.write(sql_query)  # print the SQL query
    cursor.execute(sql_query)
    cursor.close()
    conn.close()
    st.write("Data has been imported into Snowflake!")


# create file uploader
uploaded_file = st.file_uploader("UPLOAD CURRENT SALES REPORT AFTER IT HAS BEEN FORMATED", type=["xlsx"])

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




    
import streamlit as st
import pandas as pd
import base64

def create_gap_report(conn):

    # Execute SQL query and retrieve data
    query = "SELECT * FROM my_view"
    df = pd.read_sql(query, conn)

    # Create button to download Excel file
    if st.button('Download Gap Report'):
        tmp_download_link = download_link(df, 'my_data.csv', 'Click here to download your data!')
        st.markdown(tmp_download_link, unsafe_allow_html=True)

def download_link(df, filename, link_text):
    """
    Generates a link allowing the data in a given pandas dataframe to be downloaded
    in CSV format.
    """
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'data:text/csv;base64,{b64}'
    return f'<a href="{href}" download="{filename}">{link_text}</a>'

launch_gap_report("Pull Gap Report"):
    if st.button('Generate Gap Report'):
        create_gap_report(conn)
