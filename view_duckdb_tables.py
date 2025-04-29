# view_duckdb_tables.py - Script to view the contents of weather, traffic, and transformed tables in a single DuckDB file

import duckdb
import os
import pandas as pd
from dotenv import load_dotenv
import traceback

# --- Configuration ---
# Load environment variables from .env file
load_dotenv()

# Get the database path and table names from environment variables
# Defaults assume data is consolidated in traffic_data.duckdb
DUCKDB_DATABASE_PATH = os.getenv("DUCKDB_DATABASE_PATH", "traffic_data.duckdb")
WEATHER_TABLE_NAME = os.getenv("WEATHER_TABLE_NAME", "weather_data") # Default weather table name
TRAFFIC_TABLE_NAME = os.getenv("TRAFFIC_TABLE_NAME", "traffic_flow_data") # Default traffic table name
# Added configuration for the transformed table name
TRANSFORMED_TABLE_NAME = os.getenv("TRANSFORMED_TABLE_NAME", "transformed_weather_traffic") # Default transformed table name


# --- Helper Function to Query and Display a Table ---
def query_and_display_table(con, table_name: str, db_path: str):
    """
    Queries a specified table in an active DuckDB database connection and displays its contents.
    """
    print(f"\n--- Contents of table: '{table_name}' ---")

    try:
        # Check if the table exists
        # Use information_schema.tables for a robust check
        table_exists = con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[0] > 0

        if table_exists:
            # Query all data from the table
            df = con.execute(f"SELECT * FROM {table_name}").fetchdf()

            if not df.empty:
                print(f"Found {len(df)} row(s) in '{table_name}':")
                print(df)
            else:
                print(f"Table '{table_name}' is empty.")
        else:
            print(f"Table '{table_name}' does not exist in the database '{db_path}'.")

    except duckdb.Error as e:
        print(f"❌ DuckDB Error querying table '{table_name}': {e}")
        traceback.print_exc()
    except Exception as e:
        print(f"❌ An unexpected error occurred querying '{table_name}': {e}")
        traceback.print_exc()


# --- Main Execution ---
if __name__ == "__main__":
    print("Attempting to view contents of DuckDB tables...")

    db_path = DUCKDB_DATABASE_PATH
    weather_table = WEATHER_TABLE_NAME
    traffic_table = TRAFFIC_TABLE_NAME
    transformed_table = TRANSFORMED_TABLE_NAME # Get the transformed table name

    # --- Initial Checks ---
    if not db_path:
        print("❌ DUCKDB_DATABASE_PATH environment variable is not set. Exiting.")
        exit(1)
    if not weather_table:
        print("❌ WEATHER_TABLE_NAME environment variable is not set. Cannot check weather table.")
    if not traffic_table:
        print("❌ TRAFFIC_TABLE_NAME environment variable is not set. Cannot check traffic table.")
    if not transformed_table:
         print("❌ TRANSFORMED_TABLE_NAME environment variable is not set. Cannot check transformed table.")


    # Ensure the database file exists
    if not os.path.exists(db_path):
        print(f"❌ DuckDB database file not found at '{db_path}'. Please run your ETL scripts first to create it.")
        exit(1)

    duckdb_con = None
    try:
        # Connect to the DuckDB database in read-only mode
        print(f"Attempting to connect to DuckDB database: {db_path}")
        duckdb_con = duckdb.connect(database=db_path, read_only=True)
        print("✅ DuckDB connection successful (read-only).")

        # --- Query and Display Weather Table ---
        # Pass the connection and db_path to the helper function
        if weather_table:
             query_and_display_table(duckdb_con, weather_table, db_path)
        else:
             print("\nSkipping weather table check due to missing configuration.")


        # --- Query and Display Traffic Table ---
        # Pass the connection and db_path to the helper function
        if traffic_table:
             query_and_display_table(duckdb_con, traffic_table, db_path)
        else:
             print("\nSkipping traffic table check due to missing configuration.")


        # --- Query and Display Transformed Table ---
        # Added section to query and display the transformed table
        # Pass the connection and db_path to the helper function
        if transformed_table:
             query_and_display_table(duckdb_con, transformed_table, db_path)
        else:
             print("\nSkipping transformed table check due to missing configuration.")


    except duckdb.Error as e:
        print(f"❌ Failed to connect to DuckDB: {e}")
        traceback.print_exc()
    except Exception as e:
        print(f"❌ An unexpected error occurred during execution: {e}")
        traceback.print_exc()
    finally:
        if duckdb_con:
            duckdb_con.close()
            print("\n✅ DuckDB connection closed.")

    print("\nScript finished.")
