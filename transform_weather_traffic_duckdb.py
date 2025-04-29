# transform_weather_traffic_duckdb.py - Script to perform in-database transformation using DuckDB (as a function)

import duckdb
import os
import pandas as pd
from dotenv import load_dotenv
import datetime
import traceback

# --- Configuration ---
# Load environment variables (needed if this script is run standalone)
load_dotenv()

# Get the database path and table names from environment variables
# Assuming both weather and traffic data are in the same DB file for transformation
# We'll default to the traffic DB path as the transformation target
DUCKDB_DATABASE_PATH = os.getenv("DUCKDB_DATABASE_PATH", "traffic_data.duckdb") # Database file containing both tables

WEATHER_TABLE_NAME = os.getenv("WEATHER_TABLE_NAME", "weather_data") # Name of the weather table
TRAFFIC_TABLE_NAME = os.getenv("TRAFFIC_TABLE_NAME", "traffic_flow_data") # Name of the traffic table
TRANSFORMED_TABLE_NAME = os.getenv("TRANSFORMED_TABLE_NAME", "transformed_weather_traffic") # Name for the new transformed table


# --- Transformation Function ---
def run_transformation(db_path: str, weather_table: str, traffic_table: str, transformed_table: str):
    """
    Connects to DuckDB, performs the weather and traffic data transformation,
    and saves the result to a new table. Appends data if the table exists.
    """
    print("\n--- Running Transformation ---")
    print(f"Connecting to database: {db_path}")

    duckdb_con = None
    try:
        # Connect to the DuckDB database
        duckdb_con = duckdb.connect(database=db_path, read_only=False)
        print("✅ DuckDB connection successful.")

        # --- Define the SQL Transformation SELECT Query ---
        # This query joins weather and traffic data and prepares the transformed data.
        # It joins on location_name and finds the nearest weather reading in time
        # for each traffic reading.
        # The output includes the weather's location_name as the identifier.
        # This SELECT statement will be used for INSERTing data.
        transformation_select_sql = f"""
        SELECT
            -- Location Name (from weather data, as requested)
            w.location_name AS location_name, -- Using location_name from weather

            -- Traffic Data
            t.currentTravelTime / 60.0 AS transit_time_minutes, -- Convert seconds to minutes
            t.confidence AS confidence_level,
            t.extraction_timestamp AS traffic_timestamp, -- Include the original traffic timestamp

            -- Weather Data (from the nearest weather reading in time)
            w.weather_description,
            w.temperature_celsius,
            -- Removed w.fetch_timestamp_utc from the final output

            -- Metadata for the transformed record
            NOW() AS transformation_timestamp -- Timestamp of when this record was created by the transformation

        FROM {traffic_table} AS t
        JOIN LATERAL (
            SELECT
                w_inner.location_name, -- Select location_name from weather
                w_inner.weather_description,
                w_inner.temperature_celsius,
                w_inner.fetch_timestamp_utc, -- Keep this here for calculating time_diff_seconds
                -- Calculate the time difference for ordering
                ABS(EPOCH(w_inner.fetch_timestamp_utc) - EPOCH(t.extraction_timestamp)) AS time_diff_seconds
            FROM {weather_table} AS w_inner
            -- JOIN on location_name instead of lat/lon for potentially more robust matching
            WHERE w_inner.location_name = (
                SELECT location_name
                FROM {weather_table} AS w_location
                WHERE w_location.latitude = CAST(SPLIT_PART(t.point, ',', 1) AS DOUBLE)
                  AND w_location.longitude = CAST(SPLIT_PART(t.point, ',', 2) AS DOUBLE)
                LIMIT 1 -- Get the location name for the traffic point's coordinates
            )
            ORDER BY time_diff_seconds ASC -- Order by closest timestamp
            LIMIT 1 -- Take only the single closest weather reading for that location name
        ) AS w ON TRUE; -- LATERAL join syntax

        """

        # --- Explicitly Create the transformed table if it doesn't exist ---
        print(f"\nEnsuring transformed table '{transformed_table}' exists...")
        try:
            # Check if the table exists using information_schema.tables
            table_exists = duckdb_con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{transformed_table}'").fetchone()[0] > 0

            if not table_exists:
                print(f"Table '{transformed_table}' does not exist. Creating table with explicit schema...")
                # Define the CREATE TABLE statement with explicit columns and types
                create_table_explicit_sql = f"""
                CREATE TABLE {transformed_table} (
                    location_name VARCHAR,
                    transit_time_minutes DOUBLE,
                    confidence_level DOUBLE,
                    traffic_timestamp TIMESTAMP,
                    weather_description VARCHAR,
                    temperature_celsius DOUBLE,
                    transformation_timestamp TIMESTAMP
                );
                """
                duckdb_con.execute(create_table_explicit_sql)
                print(f"✅ Table '{transformed_table}' created with explicit schema.")
            else:
                print(f"Table '{transformed_table}' already exists.")

        except duckdb.Error as e:
            print(f"❌ DuckDB Error checking or creating table '{transformed_table}': {e}")
            traceback.print_exc()
            # If table creation/check fails, we cannot proceed with INSERT
            raise # Re-raise the exception


        # --- Insert the new transformed data ---
        print(f"\nInserting new transformed data into table '{transformed_table}'...")
        # Use the transformation_select_sql to insert data
        insert_sql = f"INSERT INTO {transformed_table} {transformation_select_sql};"
        duckdb_con.execute(insert_sql)
        print(f"✅ New transformed data inserted into '{transformed_table}'.")


        # --- Optional: Verify the new table ---
        print(f"\n--- Verifying contents of the table: '{transformed_table}' ---")
        try:
            table_count = duckdb_con.execute(f"SELECT COUNT(*) FROM {transformed_table}").fetchone()[0]
            print(f"Table '{transformed_table}' contains {table_count} row(s) after insertion.")

            if table_count > 0:
                 print("\nFirst 10 rows from the transformed table:") # Show a few more rows
                 transformed_df_head = duckdb_con.execute(f"SELECT * FROM {transformed_table} LIMIT 10").fetchdf()
                 print(transformed_df_head)
            else:
                print("The transformed table is empty after insertion.")

        except duckdb.CatalogException:
            print(f"❌ Table '{transformed_table}' not found after transformation.")
        except Exception as e:
             print(f"❌ Error querying transformed table: {e}")
             traceback.print_exc()


    except duckdb.Error as e:
        print(f"❌ DuckDB Error during transformation: {e}")
        traceback.print_exc()
        raise # Re-raise the exception for the caller
    except Exception as e:
        print(f"❌ An unexpected error occurred during transformation: {e}")
        traceback.print_exc()
        raise # Re-raise the exception for the caller
    finally:
        # Ensure the DuckDB connection is closed
        if duckdb_con:
            # Commit any pending transactions before closing
            try:
                duckdb_con.commit()
            except Exception as e:
                 print(f"Warning: Error during final commit: {e}")
                 traceback.print_exc()
            try:
                duckdb_con.close()
                print("\n✅ DuckDB connection closed.")
            except Exception as e:
                 print(f"Warning: Error closing DuckDB connection: {e}")
                 traceback.print_exc()


# --- Main Execution Block (for standalone testing) ---
if __name__ == "__main__":
    print("Running transform_weather_traffic_duckdb.py standalone...")
    # This block allows running the script directly for testing the transformation function
    db_path = DUCKDB_DATABASE_PATH
    weather_table = WEATHER_TABLE_NAME
    traffic_table = TRAFFIC_TABLE_NAME
    transformed_table = TRANSFORMED_TABLE_NAME

    # Ensure the database file exists before attempting transformation
    if not os.path.exists(db_path):
        print(f"❌ Database file not found at '{db_path}'. Cannot run transformation standalone.")
    else:
        try:
            run_transformation(db_path, weather_table, traffic_table, transformed_table)
        except Exception as e:
            print(f"Standalone transformation run failed: {e}")

    print("\nStandalone transformation script finished.")

