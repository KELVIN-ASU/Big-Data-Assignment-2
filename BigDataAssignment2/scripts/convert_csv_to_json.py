import pandas as pd
import os
import json
import numpy as np

# Define dataset directory
DATASET_DIR = r"C:\Users\Administrator\Desktop\BigDataAssignment2\Cleaned_Datasets"
OUTPUT_DIR = os.path.join(DATASET_DIR, "json_files")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Default values for missing data
DEFAULT_DATE = "2000-01-01 00:00:00"  # Default date value
DEFAULT_TEXT = "unknown"  # Default text value
DEFAULT_NUMBER = 0  # Default numeric value

# CSV files to process
CSV_FILES = {
    "campaigns": "campaigns_cleaned.csv",
    "client_first_purchase": "client_first_purchase_cleaned.csv",
    "events": "events_cleaned.csv",
    "friends": "friends_cleaned.csv",
    "messages": "messages_cleaned.csv"
}

# Function to clean and format data
def clean_data(df):
    """
    Cleans the dataframe by:
    - Filling NaN values: numbers → 0, text → "unknown", dates → DEFAULT_DATE
    - Ensuring correct data types
    """
    # Convert empty strings and "NaN" to actual NaN values
    df.replace({"NaN": np.nan, "": np.nan}, inplace=True)

    # Fill numeric columns with 0
    for col in df.select_dtypes(include=["number"]).columns:
        df[col].fillna(DEFAULT_NUMBER, inplace=True)

    # Fill object (string) columns with "unknown"
    for col in df.select_dtypes(include=["object"]).columns:
        df[col].fillna(DEFAULT_TEXT, inplace=True)

    # Fill date columns with DEFAULT_DATE
    for col in df.columns:
        if "date" in col.lower() or "time" in col.lower():  # Detect date-like columns
            df[col].fillna(DEFAULT_DATE, inplace=True)

    return df

# Function to split large JSON files
def save_large_json(data, json_path, max_records=100000):
    file_count = 1
    for i in range(0, len(data), max_records):
        split_path = json_path.replace(".json", f"_{file_count}.json")
        with open(split_path, "w", encoding="utf-8") as f:
            json.dump(data[i:i+max_records], f, indent=2)
        print(f"Saved {len(data[i:i+max_records])} records to {split_path}")
        file_count += 1

# Convert CSV to JSON with cleaning
for collection, filename in CSV_FILES.items():
    csv_path = os.path.join(DATASET_DIR, filename)
    json_path = os.path.join(OUTPUT_DIR, f"{collection}.json")

    print(f"Processing {csv_path}...")

    df = pd.read_csv(csv_path, dtype=str)  # Read as strings initially
    df = clean_data(df)  # Apply cleaning

    records = df.to_dict(orient="records")

    # If dataset is too large, split into multiple JSON files
    if len(records) > 100000:
        save_large_json(records, json_path)
    else:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2)

    print(f"Cleaned and Converted {csv_path} → {json_path}")

print("🎉 All CSV files cleaned and converted to JSON successfully!")
