from bs4 import BeautifulSoup
import requests
import numpy as np
import pandas as pd 
import sqlite3
from datetime import datetime

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ['Rank', 'Bank Name', 'Market Cap(US$ Billion)']
csv = "exchange_rate.csv"
output = "banks_MC.csv"
table_name = 'Largest_banks'
query_list = ['SELECT * FROM Largest_banks;','SELECT AVG(MC_GBP_Billion) FROM Largest_banks;','SELECT bank_name from Largest_banks LIMIT 5;']

def log_progress(message): # used to log script process to cde_log.txt file
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')
def extract(url, table_attribs): # extracts data from html and converts to a dataframe
    html = requests.get(url).text
    df = pd.DataFrame(columns = table_attribs)
    data = BeautifulSoup(html, 'html.parser')
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    rank = []
    bank_names = []
    market_cap = []
    for row in rows:
        col = row.find_all('td')
        if len(col)!= 0:
            bank_names.append(col[1].find_all('a')[1]['title'])
            rank.append(col[0].contents[0][:-1])
            market_cap.append(float(col[2].contents[0][:-1]))

            # rank.append(col[0].text.strip()) these will do the same thing
            # bank_names.append(col[1].text.strip())
            # market_cap.append(float(col[2].text.strip()))

    bank_columns  = {"Rank" : rank, "Bank_name": bank_names, "Market Cap(US$ Billion)": market_cap}
    df = pd.DataFrame(bank_columns)

    return df
def transform(df,csv): # ues info from csv to convert all data under Market Cap column and inserts that data along with corresponding currency as new column for that bank
    csv_df = pd.read_csv("exchange_rate.csv")
    dict = csv_df.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*dict['GBP'],2) for x in df['Market Cap(US$ Billion)']]
    df['MC_EUR_Billion'] = [np.round(x*dict['EUR'],2) for x in df['Market Cap(US$ Billion)']]
    df['MC_INR_Billion'] = [np.round(x*dict['INR'],2) for x in df['Market Cap(US$ Billion)']]
    
    return df

def load_to_csv(df, output): # loads final dataframe to csv file
    df.to_csv(output)

    return df

def load_to_db(df, table_name): # loads that dataframe to DB as new table
    conn = sqlite3.connect('Banks.db')
    df.to_sql(table_name, conn, if_exists = 'replace', index = False)
def run_queries(query_list): # queries that table and prints each query's output 
    conn = sqlite3.connect('Banks.db')
    for query in query_list:
        print(query)
        query_ouput = pd.read_sql(query, conn)
        print(query_ouput)
    conn.close()


log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df,csv)
log_progress('Data transformation complete. Initiating Loading process')
df = load_to_csv(df,output)
log_progress('Data saved to CSV file')
log_progress('SQL Connection initiated')
load_to_db(df, table_name)
log_progress('Data loaded to Database as a table, Executing queries')
run_queries(query_list)
log_progress('Process Complete')
log_progress('Server Connection closed')


