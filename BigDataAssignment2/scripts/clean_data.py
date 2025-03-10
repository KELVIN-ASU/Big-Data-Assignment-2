import os
import pandas as pd
import numpy as np
import uuid

# Define dataset paths
script_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(script_dir, "..", "Datasets")
output_dir = os.path.join(script_dir, "..", "Cleaned_Datasets")
os.makedirs(output_dir, exist_ok=True)

def clean_campaigns():
    print("Cleaning campaigns.csv...")
    df = pd.read_csv(os.path.join(data_dir, "campaigns.csv"))
    
    # Enforce schema data types
    df["id"] = pd.to_numeric(df["id"], errors='coerce')
    df["total_count"] = pd.to_numeric(df["total_count"], errors='coerce')
    df["hour_limit"] = pd.to_numeric(df["hour_limit"], errors='coerce')
    df["subject_length"] = pd.to_numeric(df["subject_length"], errors='coerce')
    df["position"] = pd.to_numeric(df["position"], errors='coerce')
    
    # Convert date columns to datetime
    df["started_at"] = pd.to_datetime(df["started_at"], errors='coerce')
    df["finished_at"] = pd.to_datetime(df["finished_at"], errors='coerce')
    
    # Remove duplicates & save
    df.drop_duplicates(inplace=True)
    df.to_csv(os.path.join(output_dir, "campaigns_cleaned.csv"), index=False)
    print("Finished cleaning campaigns.csv.\n")

def clean_client_first_purchase():
    print("Cleaning client_first_purchase_date.csv...")
    df = pd.read_csv(os.path.join(data_dir, "client_first_purchase_date.csv"))
    
    # Enforce data types
    df["client_id"] = pd.to_numeric(df["client_id"], errors='coerce', downcast='integer')
    df["user_id"] = pd.to_numeric(df["user_id"], errors='coerce', downcast='integer')
    df["user_device_id"] = pd.to_numeric(df["user_device_id"], errors='coerce')
    
    # Convert date column
    df["first_purchase_date"] = pd.to_datetime(df["first_purchase_date"], errors='coerce')
    
    # Remove duplicates & save
    df.drop_duplicates(inplace=True)
    df.to_csv(os.path.join(output_dir, "client_first_purchase_cleaned.csv"), index=False)
    print("Finished cleaning client_first_purchase_date.csv.\n")

def clean_events():
    print("Cleaning events.csv...")
    df = pd.read_csv(os.path.join(data_dir, "events.csv"))
    
    # Enforce data types
    df["product_id"] = pd.to_numeric(df["product_id"], errors='coerce')
    df["category_id"] = pd.to_numeric(df["category_id"], errors='coerce')
    df["price"] = pd.to_numeric(df["price"], errors='coerce')
    df["user_id"] = pd.to_numeric(df["user_id"], errors='coerce', downcast='integer')
    
    # Convert event_time to datetime
    df["event_time"] = pd.to_datetime(df["event_time"], errors='coerce')
    
    # Filter to only valid user_ids
    valid_users = pd.read_csv(os.path.join(output_dir, "client_first_purchase_cleaned.csv"))["user_id"].tolist()
    df = df[df["user_id"].isin(valid_users)]
    
    # Remove duplicates & save
    df.drop_duplicates(inplace=True)
    df.to_csv(os.path.join(output_dir, "events_cleaned.csv"), index=False)
    print("Finished cleaning events.csv.\n")

def clean_friends():
    print("Cleaning friends.csv...")
    df = pd.read_csv(os.path.join(data_dir, "friends.csv"))
    
    # Enforce data types
    df["friend1"] = pd.to_numeric(df["friend1"], errors='coerce', downcast='integer')
    df["friend2"] = pd.to_numeric(df["friend2"], errors='coerce', downcast='integer')
    
    # Filter to only valid user_ids
    valid_users = pd.read_csv(os.path.join(output_dir, "client_first_purchase_cleaned.csv"))["user_id"].tolist()
    df = df[(df["friend1"].isin(valid_users)) & (df["friend2"].isin(valid_users))]
    
    # Remove duplicates & save
    df.drop_duplicates(inplace=True)
    df.to_csv(os.path.join(output_dir, "friends_cleaned.csv"), index=False)
    print("Finished cleaning friends.csv.\n")

def clean_messages():
    print("Cleaning messages.csv...")
    df = pd.read_csv(os.path.join(data_dir, "messages.csv"), low_memory=False)
    
    # Enforce data types
    df["campaign_id"] = pd.to_numeric(df["campaign_id"], errors='coerce')
    df["client_id"] = pd.to_numeric(df["client_id"], errors='coerce', downcast='integer')
    df["user_device_id"] = pd.to_numeric(df["user_device_id"], errors='coerce')
    df["user_id"] = pd.to_numeric(df["user_id"], errors='coerce', downcast='integer')
    
    # Convert UUID field to valid format
    df["message_id"] = df["message_id"].apply(lambda x: str(uuid.UUID(x)) if pd.notna(x) and isinstance(x, str) else None)
    
    # Convert date columns to datetime
    date_cols = ["date", "sent_at", "opened_first_time_at", "opened_last_time_at", "clicked_first_time_at", "clicked_last_time_at", "unsubscribed_at", "hard_bounced_at", "soft_bounced_at", "complained_at", "blocked_at", "purchased_at", "created_at", "updated_at"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Filter to only valid client_ids
    valid_clients = pd.read_csv(os.path.join(output_dir, "client_first_purchase_cleaned.csv"))["client_id"].tolist()
    df = df[df["client_id"].isin(valid_clients)]
    
    # Remove duplicates & save
    df.drop_duplicates(inplace=True)
    df.to_csv(os.path.join(output_dir, "messages_cleaned.csv"), index=False)
    print("Finished cleaning messages.csv.\n")

def main():
    print("Starting data cleaning process...\n")
    clean_campaigns()
    clean_client_first_purchase()
    clean_events()
    clean_friends()
    clean_messages()
    print("All datasets cleaned successfully!\n")

if __name__ == "__main__":
    main()
