import unittest
import os
def test_download_db_file(file_path):
        
        if os.path.exists(file_path):
                print("db file exist", file_path)
        else:
                print ("Error: Can't find file path ", file_path)

test_download_db_file ("data/items_db.db")
test_download_db_file ("data/transactions_data.db")
