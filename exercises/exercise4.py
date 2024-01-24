import urllib.request
import zipfile
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import BIGINT, FLOAT, TEXT

# Define the URL and local file names
url = "https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip"
zip_file_path = "mowesta-dataset.zip"
extracted_folder = "mowesta-dataset"

# Download the ZIP file
urllib.request.urlretrieve(url, zip_file_path)

# Extract the contents
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(extracted_folder)

# Set the file path for the CSV
#
# csv_file_path = os.path.join(extracted_folder, "data.csv")
    
df = pd.read_csv(os.path.join(extracted_folder, "data.csv"),
                     sep=';', index_col=False,
                     usecols=['Geraet', 'Hersteller', 'Model', 'Monat', 'Temperatur in °C (DWD)',
                              'Batterietemperatur in °C', 'Geraet aktiv'], decimal=',')

# Read the CSV file
#df = pd.read_csv(csv_file_path)

# Select relevant columns
#selected_columns = ["Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)", "Batterietemperatur in °C", "Geraet aktiv"]
#df = df[selected_columns]

# Rename columns
df.rename(columns={"Temperatur in °C (DWD)": "Temperatur", "Batterietemperatur in °C": "Batterietemperatur"}, inplace=True)

# Discard columns to the right of "Geraet aktiv"
last_index = df.columns.get_loc("Geraet aktiv")
df = df.iloc[:, :last_index + 1]

# Transform temperatures to Fahrenheit
df["Temperatur"] = (df["Temperatur"] * 9/5) + 32
df["Batterietemperatur"] = (df["Batterietemperatur"] * 9/5) + 32

# Validate "Geraet" to be an id over 0
assert df["Geraet"].min() > 0

# Write data types to SQLite
#column_types = {"Geraet": "BIGINT", "Hersteller": "TEXT", "Model": "TEXT", "Monat": "TEXT",
     #           "Temperatur": "FLOAT", "Batterietemperatur": "FLOAT", "Geraet aktiv": "TEXT"}
#df = df.astype(column_types)

# Define SQLite database path
#db_path = "temperatures.sqlite"

# Create SQLite engine and write data to database
#engine = create_engine(f"sqlite:///{db_path}")
#df.to_sql("temperatures", engine, index=False, if_exists="replace")
table = 'temperatures'
database = 'temperatures.sqlite'
df.to_sql(table, f'sqlite:///{database}', if_exists='replace', index=False, dtype={
        'Geraet': BIGINT, 'Hersteller': TEXT, 'Model': TEXT, 'Monat': BIGINT,
        'Temperatur': FLOAT, 'Batterietemperatur': FLOAT, 'Geraet aktiv': TEXT
    })

