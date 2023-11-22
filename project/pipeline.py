import zipfile
import shutil
import pandas as pd
import sqlite3
from kaggle.api.kaggle_api_extended import KaggleApi

def extract_zip(old_name: str, new_name: str):
    shutil.move(old_name, new_name)
    with zipfile.ZipFile(new_name, 'r') as zip_ref:
        zip_ref.extractall('items')

def read_and_preprocess(file_path, nan_fill_value=0, encoding='latin-1'):
    data = pd.read_csv(file_path, encoding=encoding)
    data.fillna(nan_fill_value, inplace=True)
    return data

def save_to_csv_and_sql(data, csv_path, sql_path, table_name):
    data.to_csv(csv_path, index=False)
    conn = sqlite3.connect(sql_path)
    data.to_sql(table_name, conn, index=False, if_exists='replace')
    conn.close()

if __name__ == "__main__":
    api = KaggleApi()
    api.authenticate()

    # download the datasets
    api.dataset_download_file('oscarm524/book-recommendation/data','items.csv')
    
    extract_zip('items.csv.zip', 'items.zip')

    # read the both datasets
    item_data = read_and_preprocess('items.csv')
    transaction_data = read_and_preprocess('transactions.csv')

    # Read and preprocess Renewable Energy dataset
    renewables_data = read_and_preprocess('transactions.csv', nan_fill_value=0)
    transaction_data = transaction_data.rename(columns={'Entity': 'itemID'})

    # Save datasets to CSV and SQLite
    save_to_csv_and_sql(item_data, 'data/items.csv', 'data/item.sqlite', 'item')
    save_to_csv_and_sql(transaction_data, 'data/transactions.csv', 'data/transaction.sqlite', 'transaction_data')