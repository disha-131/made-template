import zipfile
import shutil
import pandas as pd
import sqlite3
from kaggle.api.kaggle_api_extended import KaggleApi

def extract_and_move(old_name: str, new_name: str):
    shutil.move(old_name, new_name)
    with zipfile.ZipFile(new_name, 'r') as zip_ref:
        zip_ref.extractall('items_zip')

def extract_and_move_evaluation(old_name: str, new_name: str):
    shutil.move(old_name, new_name)
    with zipfile.ZipFile(new_name, 'r') as zip_ref:
        zip_ref.extractall('transactions_zip')

def load_and_fill_missing(file_path, nan_fill_value=0, encoding='latin-1'):
    data = pd.read_csv(file_path, encoding=encoding,delimiter='|')
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
    
    api.dataset_download_file('oscarm524/book-recommendation/data','items.csv')
    api.dataset_download_file('oscarm524/book-recommendation/data','transactions.csv')

    extract_and_move('items.csv.zip', 'items_zip.zip')
    extract_and_move_evaluation('transactions.csv.zip', 'transactions_zip.zip')

    co2_data = load_and_fill_missing('items_zip/items.csv')
    transactions_data = load_and_fill_missing('transactions_zip/transactions.csv')
    
    save_to_csv_and_sql(co2_data, 'data/items.csv', 'data/items_db.db', 'items')
    save_to_csv_and_sql(transactions_data, 'data/transactions.csv', 'data/transactions_data.db', 'transactions_data')