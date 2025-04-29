# load_traffic_duckdb.py

import duckdb
import pandas as pd
import datetime
import traceback

# --- Data Loading Function (DuckDB) ---

def load_dataframe_to_duckdb(con, df: pd.DataFrame, table_name: str, point_identifier: str):
    """
    Loads a pandas DataFrame into a DuckDB table.
    Adds 'point' and 'extraction_timestamp' metadata columns.
    Creates the table if it does not exist, inferring schema from the DataFrame.

    Args:
        con: Active DuckDB connection object.
        df (pd.DataFrame): The DataFrame containing data to load.
        table_name (str): The name of the target table in the DuckDB database.
        point_identifier (str): The 'lat,lon' string identifying the geographic point.
    """
    if df.empty:
        print("🚫 DataFrame is empty, skipping load to DuckDB.")
        return

    print(f"Attempting to load data into DuckDB table '{table_name}'...")

    # Add metadata columns to the DataFrame before loading
    df['point'] = point_identifier # Geographic point identifier
    df['extraction_timestamp'] = datetime.datetime.now() # Timestamp of data extraction

    try:
        # Check if the target table exists in DuckDB
        table_exists = False
        try:
            # Use PRAGMA table_info to check for table existence without erroring if not found
            con.execute(f"PRAGMA table_info('{table_name}')")
            table_exists = True
            print(f"Table '{table_name}' already exists.")
        except duckdb.CatalogException:
            # Table does not exist, proceed to create it
            print(f"Table '{table_name}' does not exist. Creating table...")
            # Create the table schema based on the DataFrame structure but insert no data (WHERE 1=0)
            # This ensures the table has the correct columns as per the current DataFrame structure
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df WHERE 1=0;")
            print(f"Table '{table_name}' created.")
            table_exists = True # Table is now created

        # If table creation failed or it didn't exist and couldn't be created
        if not table_exists:
             print(f"❌ Failed to create or find table '{table_name}'. Cannot insert data.")
             return

        # Insert data from the DataFrame into the table
        # This relies on column names matching between the DataFrame and the table schema
        con.execute(f"INSERT INTO {table_name} SELECT * FROM df")

        print(f"✅ Successfully loaded {len(df)} row(s) into '{table_name}'.")

    except duckdb.Error as e:
        print(f"❌ DuckDB Error loading data: {e}")
        traceback.print_exc()
    except Exception as e:
        print(f"❌ An unexpected error occurred during DuckDB load: {e}")
        traceback.print_exc()

