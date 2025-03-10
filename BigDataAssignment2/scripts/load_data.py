import os
import pandas as pd
import pg8000
import numpy as np
import uuid

# Define dataset paths
script_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(script_dir, "..", "Cleaned_Datasets")

# Database connection
DB_PARAMS = {
    "database": "ecommerce",
    "user": "postgres",
    "password": "kera1998",
    "host": "localhost",
    "port": 5432
}

def load_to_db(table_name, df):
    """Loads a Pandas DataFrame to PostgreSQL using pg8000, ensuring NaN values are handled properly."""
    conn = pg8000.connect(**DB_PARAMS)
    cur = conn.cursor()
    
    # Replace NaN values with appropriate defaults for PostgreSQL
    df = df.replace({np.nan: None})
    
    # Convert UUID columns to valid format
    if "message_id" in df.columns:
        df["message_id"] = df["message_id"].apply(lambda x: str(uuid.UUID(x)) if pd.notna(x) else None)
    
    # Generate insert query
    placeholders = ', '.join(['%s'] * len(df.columns))
    query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
    
    # Convert dataframe to list of tuples and execute query
    data = [tuple(x) for x in df.to_numpy()]
    cur.executemany(query, data)
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"Successfully loaded data into {table_name}.")

def load_campaigns():
    print("Loading campaigns.csv...")
    df = pd.read_csv(os.path.join(data_dir, "campaigns_cleaned.csv"))
    load_to_db("campaigns", df)

def load_client_first_purchase():
    print("Loading client_first_purchase_date.csv...")
    df = pd.read_csv(os.path.join(data_dir, "client_first_purchase_cleaned.csv"))
    load_to_db("client_first_purchase", df)

def load_events():
    print("Loading events.csv...")
    df = pd.read_csv(os.path.join(data_dir, "events_cleaned.csv"))
    
    # Fetch valid user_id values from client_first_purchase
    conn = pg8000.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM client_first_purchase;")
    valid_user_ids = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    
    # Filter events to only include existing user_ids
    df = df[df["user_id"].isin(valid_user_ids)]
    
    load_to_db("events", df)

def load_friends():
    print("Loading friends.csv...")
    df = pd.read_csv(os.path.join(data_dir, "friends_cleaned.csv"))
    
    # Fetch valid user_id values from client_first_purchase
    conn = pg8000.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM client_first_purchase;")
    valid_user_ids = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    
    # Filter friends to only include existing user_ids
    df = df[(df["friend1"].isin(valid_user_ids)) & (df["friend2"].isin(valid_user_ids))]
    
    load_to_db("friends", df)

def load_messages():
    print("Loading messages.csv...")
    df = pd.read_csv(os.path.join(data_dir, "messages_cleaned.csv"), low_memory=False)
    
    # Fetch valid client_id values from client_first_purchase
    conn = pg8000.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("SELECT client_id FROM client_first_purchase;")
    valid_client_ids = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    
    # Filter messages to only include existing client_ids
    df = df[df["client_id"].isin(valid_client_ids)]
    
    load_to_db("messages", df)

def main():
    print("Starting data loading process...\n")
    load_campaigns()
    load_client_first_purchase()
    load_events()
    load_friends()
    load_messages()
    print("All datasets loaded successfully!\n")

if __name__ == "__main__":
    main()
