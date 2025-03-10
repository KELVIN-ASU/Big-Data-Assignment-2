import os
import pg8000
import pandas as pd

# Database connection parameters
DB_PARAMS = {
    "database": "ecommerce",
    "user": "postgres",
    "password": "kera1998",
    "host": "localhost",
    "port": 5432
}

# Define paths
script_dir = os.path.dirname(os.path.abspath(__file__))
queries_dir = os.path.join(script_dir)
output_dir = os.path.join(script_dir, "..", "Analysis_Results")
os.makedirs(output_dir, exist_ok=True)

# Function to run SQL queries and save results
def run_query(query_file, output_file):
    """Executes SQL query from file and saves the result to CSV."""
    conn = pg8000.connect(**DB_PARAMS)
    cur = conn.cursor()

    # Read the SQL query
    with open(os.path.join(queries_dir, query_file), 'r') as f:
        query = f.read()

    try:
        # Execute query
        cur.execute(query)
        results = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]

        # Convert to DataFrame
        df = pd.DataFrame(results, columns=column_names)

        # Save results to CSV
        output_path = os.path.join(output_dir, output_file)
        df.to_csv(output_path, index=False)
        print(f"Analysis saved: {output_file}")

        # Display results
        print(df.head())

    except Exception as e:
        print(f"Error executing {query_file}: {e}")

    finally:
        cur.close()
        conn.close()

# Run all analysis queries
def main():
    print("Starting PostgreSQL Data Analysis...\n")
    
    run_query("q1.sql", "campaign_effectiveness.csv")
    run_query("q2.sql", "personalized_recommendations.csv")
    run_query("q3.sql", "full_text_search_results.csv")

    print("\nData analysis completed and saved!")

if __name__ == "__main__":
    main()
