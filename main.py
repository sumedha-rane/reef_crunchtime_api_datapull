import time

import requests
import pandas as pd
import paramiko
from sshtunnel import SSHTunnelForwarder
import psycopg2
from psycopg2 import sql
import pandas as pd

# API URLs
recipe_url = "https://webservices.net-chef.com/recipe/v2/getRecipesEnhancedByPage"

# API Tokens and Credentials
authenticationtoken = "8e8c1b0f-4ff5-4e3b-9284-8063d994449d"
sitename = "reefos"
user = "BFURKAN"
password = "Welcome1!"

# Headers with tokens and credentials
headers = {
    "accept": "application/json",
    "authenticationtoken": authenticationtoken,
    "sitename": sitename,
    "userid": user,
    "password": password
}

def fetch_total_pages(headers):
    url = f"{recipe_url}?page=1&limit=1"
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an exception for HTTP errors
    data = response.json()
    # Assuming the total number of pages is included in the metadata
    total_pages = data.get('totalPages', 0)
    return total_pages

def fetch_page(page, headers):
    url = f"{recipe_url}?page={page}&limit=100"
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()

def fetch_data_all_pages(total_pages, headers):
    all_data = []
    for page in range(1, total_pages + 1):
        print(f"Fetching page {page}/{total_pages}...")
        data = fetch_page(page, headers)
        if 'recipeEnhancedDetails' not in data or not data['recipeEnhancedDetails']:
            break
        all_data.extend(data['recipeEnhancedDetails'])
        print(f"Page {page} Data: {data['recipeEnhancedDetails']}")

    # Create a DataFrame from the collected data
    df = pd.json_normalize(all_data)
    return df

# Fetch total number of pages
total_pages = fetch_total_pages(headers)
print(f"Total number of pages: {total_pages}")

# Fetch data from all pages and create a DataFrame
df = fetch_data_all_pages(total_pages, headers)

# Optionally, save the DataFrame to a CSV file
df.to_csv('recipes_data.csv', index=False)

# # Extract the first two columns and two rows
# df_small = df.iloc[:2, :2]
df.columns = df.columns.str.replace('recipeEnhancedHeaderDetails.', '', regex=False)

print("\nCombined DataFrame:")
print(df.head())


# SSH Tunnel details
ssh_tunnel = SSHTunnelForwarder(
    ('hexagon.reefos.ai', 22),
    ssh_username='ec2-user',
    ssh_pkey=r'C:\Users\sumedha.rane\OneDrive - REEF Technology\Documents\SSH Keys\id_rsa.ppk',
    remote_bind_address=('hexagon-db.ctjs9tawr4c7.us-east-1.rds.amazonaws.com', 5432)
)

try:
    ssh_tunnel.start()
    print("SSH tunnel established...")

    # PostgreSQL connection details
    conn = psycopg2.connect(
        dbname='hexagon_production',
        user='hexagon',
        password='Reef123.',
        host='localhost',  # Tunnel forwards to localhost
        port=ssh_tunnel.local_bind_port
    )

    cursor = conn.cursor()

    # Lowercase all column names in the DataFrame
    df.columns = df.columns.str.lower()

    # Assuming connection is already established
    for index, row in df.iterrows():
        # Extract available columns for this row
        available_columns = [col for col in df.columns if col in row and pd.notnull(row[col])]

        # Create the insert query dynamically based on available columns
        insert_query = sql.SQL(
            "INSERT INTO crunchtime.crunchtime_receipe_details ({}) VALUES ({})"
        ).format(
            sql.SQL(', ').join(map(sql.Identifier, available_columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(available_columns))
        )

        # Execute the insert query with the values for the available columns
        cursor.execute(insert_query, tuple(row[col] for col in available_columns))

    # Commit changes to the database


    conn.commit()

except Exception as e:
    print(f"Error connecting to the database: {e}")
    time.sleep(5)  # Retry after 5 seconds if needed

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    if ssh_tunnel:
        ssh_tunnel.stop()
