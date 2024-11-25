import pandas as pd 
import numpy as np 
from bs4 import BeautifulSoup
import requests
import sqlite3
from datetime import datetime

def extract(url,table_attribs):
    page=requests.get(url).text
    data=BeautifulSoup(page,'html.parser')
    df=pd.DataFrame(columns=table_attribs)
    tables=data.find_all('tbody')
    rows=tables[0].find_all('tr')
    for row in rows:
        col=row.find_all('td')
        if len(col)!=0:
            data_dict={"Name": col[1].get_text(strip=True),"MC_USD_Billion": col[2].get_text(strip=True)}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df
def transform(df,csv_path):
    exchange_df=pd.read_csv(csv_path)
    exchange_dict=exchange_df.set_index('Currency').to_dict()['Rate']
    MC_USD_list=df["MC_USD_Billion"].tolist()
    MC_USD_list = [float("".join(x.split(','))) for x in MC_USD_list]
    df['MC_GBP_Billion'] = [np.round(x*exchange_dict['GBP'],2) for x in MC_USD_list]
    df['MC_EUR_Billion'] = [np.round(x*exchange_dict['EUR'],2) for x in MC_USD_list]
    df['MC_INR_Billion'] = [np.round(x*exchange_dict['INR'],2) for x in MC_USD_list]
    return df

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
def log_progress(message):
    time_stamp_format='%Y-%h-%d-%H:%M-%S'
    now=datetime.now()
    timestamp=now.strftime(time_stamp_format)
    with open(log_file,'a') as f:
        f.write(timestamp + ',' + message + '\n')
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

url='https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs=['Name','MC_USD_Billion']
Output_csv_path='./Largest_banks_data.csv'
Table_name='Largest_banks'
log_file='code_log.txt'
databse_name='Banks.db'

log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df,"exchange_rate.csv")
log_progress('Data transformation complete. Initiating Loading process')
load_to_csv(df, Output_csv_path)
log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect('Banks.db')
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, Table_name)
log_progress('Data loaded to Database as table. Executing queries')
query_statement1 = f"SELECT * FROM {Table_name}"
run_query(query_statement1, sql_connection)
query_statement2=f"SELECT AVG(MC_GBP_Billion) FROM {Table_name}"
run_query(query_statement2, sql_connection)
query_statement3=f"SELECT Name from {Table_name} LIMIT 5"
run_query(query_statement3, sql_connection)
log_progress('Process Complete.')
sql_connection.close()


