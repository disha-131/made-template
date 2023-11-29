import time
import pandas as pd
import sqlalchemy


def extract_csv_from_url(url: str, max_attempts: int = 5, wait_time_before_retry: float = 5) -> pd.DataFrame:
    dataframe = None

    for attempt in range(1, max_attempts + 1):
        try:
            dataframe = pd.read_csv(url, sep=';', decimal=',')
            break
        except Exception as e:
            print(f'Failed to extract CSV from the provided URL! (Attempt {attempt}/{max_attempts})')
            if attempt < max_attempts:
                time.sleep(wait_time_before_retry)

    if dataframe is None:
        raise Exception(f'Failed to extract CSV from the provided URL: {url}')

    return dataframe


# Direct link to CSV
csv_url = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"

df = extract_csv_from_url(csv_url)
# Drop the "Status" column
df.drop("Status", axis=1, inplace=True)

# Drop rows with invalid values
df = df[df["Verkehr"].isin(["FV", "RV", "nur DPN"])]

# Filter rows with valid "Laenge" and "Breite" values
df = df[(df["Laenge"].between(-90, 90)) & (df["Breite"].between(-90, 90))]


# Define a function to check the validity of "IFOPT" values
def is_valid_ifopt(value):
    if pd.isna(value):
        return False
    parts = value.split(":")
    return len(parts) >= 3 and all(part.isdigit() for part in parts[1:])


# Filter rows with valid "IFOPT" values
df = df[df["IFOPT"].apply(is_valid_ifopt)]

# Write DataFrame to SQLite
df.to_sql('trainstops', 'sqlite:///trainstops.sqlite', if_exists='replace', index=False, dtype={
    "EVA_NR": sqlalchemy.BIGINT,
    "DS100": sqlalchemy.TEXT,
    "IFOPT": sqlalchemy.TEXT,
    "NAME": sqlalchemy.TEXT,
    "Verkehr": sqlalchemy.TEXT,
    "Laenge": sqlalchemy.FLOAT,
    "Breite": sqlalchemy.FLOAT,
    "Betreiber_Name": sqlalchemy.TEXT,
    "Betreiber_Nr": sqlalchemy.BIGINT
})