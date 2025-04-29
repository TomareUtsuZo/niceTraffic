# demonstrate_weather_etl_run.py - Demonstrates running weather data extraction and loading

import duckdb
import os
import sys
import pandas as pd
from dotenv import load_dotenv
import datetime
import traceback

# --- Path Setup ---
# Get the absolute path of the directory where the script is being run (project root)
# and add it to sys.path so modules like extract_weather_duckdb can be found.
project_root = os.path.abspath(os.getcwd())
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the necessary functions and CONFIG from your weather script
try:
    # Import functions and configuration variables from extract_weather_duckdb.py
    from extract_weather_duckdb import (
        construct_weather_api_url,
        fetch_data_from_api,
        parse_weather_response_to_dataframe,
        save_weather_to_duckdb,
        WEATHER_API_KEY, # Import config variables
        WEATHER_API_BASE_URL,
        DUCKDB_DATABASE_PATH,
        WEATHER_TABLE_NAME,
        API_TIMEOUT_SECONDS
    )

except ImportError as e:
    print(f"Error: Could not import necessary functions or config from extract_weather_duckdb.py.")
    print(f"Ensure the file exists in the project root ({project_root}) and there are no other import issues.")
    print(f"Details: {e}")
    sys.exit(1) # Exit if the main script cannot be imported


# --- Helper function to get table state ---
def get_table_state(con, table_name):
    """
    Connects to DuckDB and gets the current row count and first few rows
    of a specified table. Returns count and DataFrame.
    """
    count = 0
    df_head = pd.DataFrame()
    table_exists = False

    try:
        # Check if the table exists first
        try:
            con.execute(f"PRAGMA table_info('{table_name}')")
            table_exists = True
        except duckdb.CatalogException:
            # Table does not exist
            return 0, pd.DataFrame(), False # Return count 0, empty df, and exists=False

        if table_exists:
            count_result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            if count_result:
                count = count_result[0]

            # Get first 5 rows
            try:
                # Fetch all columns as they currently exist in the table
                df_head = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()
            except Exception as e:
                print(f"Warning: Could not fetch sample rows from '{table_name}': {e}")
                df_head = pd.DataFrame() # Ensure it's an empty DataFrame on error


    except duckdb.Error as e:
        print(f"❌ DuckDB Error getting table state for '{table_name}': {e}")
        count = -1 # Indicate error
        df_head = pd.DataFrame()
        table_exists = False
    except Exception as e:
        print(f"❌ An unexpected error occurred while getting table state for '{table_name}': {e}")
        count = -1 # Indicate error
        df_head = pd.DataFrame()
        table_exists = False

    return count, df_head, table_exists


# --- Helper function to show newly added rows ---
def show_newly_added_rows(con, table_name, run_timestamp):
    """
    Queries DuckDB to show rows added during the current run based on timestamp.
    Assumes 'fetch_timestamp_utc' column exists and is a datetime type.
    """
    print(f"\n--- Rows added during this run (Timestamp >= {run_timestamp.isoformat()}) ---")
    try:
        # Use 'fetch_timestamp_utc' as the column name for weather data
        # Ensure the timestamp is timezone-aware UTC for comparison if the column is UTC
        run_timestamp_utc = run_timestamp.astimezone(datetime.timezone.utc)

        query = f"""
        SELECT *
        FROM {table_name}
        WHERE fetch_timestamp_utc >= ?
        """
        # Using parameter binding for the timestamp
        new_rows_df = con.execute(query, [run_timestamp_utc]).fetchdf()

        if not new_rows_df.empty:
            print(f"Added {len(new_rows_df)} new row(s):")
            print(new_rows_df)
        else:
            print("No new rows found with the current run timestamp.")

    except duckdb.CatalogException:
        print(f"Table '{table_name}' does not exist, cannot show new rows.")
    except duckdb.Error as e:
        print(f"❌ DuckDB Error querying for new rows: {e}")
        traceback.print_exc() # Print traceback for DuckDB errors
    except Exception as e:
        print(f"❌ An unexpected error occurred while showing new rows: {e}")
        traceback.print_exc() # Print traceback for other errors


# --- Main Demonstration Logic ---
if __name__ == "__main__":
    print("Running weather data ETL demonstration script...")

    # Load environment variables
    load_dotenv()

    # Capture the timestamp before the ETL run starts
    # Use timezone-aware datetime for consistency with the data
    run_start_timestamp = datetime.datetime.now(datetime.timezone.utc)
    print(f"Run started at: {run_start_timestamp.isoformat()}")


    # --- Define Locations to Extract ---
    # Define the list of locations you want to fetch weather data for.
    # Each item is a dictionary with 'lat', 'lon', and optionally 'name'.
    LOCATIONS_TO_EXTRACT = [
        {"lat": 10.79187, "lon": 106.68831, "name": "Ho Chi Minh City"}, # Saigon
        {"lat": 40.7128, "lon": -74.0060, "name": "New York City"},
        {"lat": 51.5074, "lon": -0.1278, "name": "London"},
        # Add more locations as needed
    ]

    # --- Get Configuration from Imported Variables ---
    api_key = WEATHER_API_KEY
    base_url = WEATHER_API_BASE_URL
    db_path = DUCKDB_DATABASE_PATH
    table_name = WEATHER_TABLE_NAME
    api_timeout = API_TIMEOUT_SECONDS

    # --- Initial Checks ---
    if not api_key:
        print("❌ WEATHER_API_KEY environment variable is not set. Exiting.")
        sys.exit(1) # Use sys.exit in scripts
    if not base_url:
         print("❌ WEATHER_API_BASE_URL environment variable is not set. Exiting.")
         sys.exit(1)
    if not db_path:
         print("❌ DUCKDB_DATABASE_PATH environment variable is not set. Exiting.")
         sys.exit(1)

    # Ensure the directory for the DuckDB file exists
    db_directory = os.path.dirname(db_path)
    if db_directory and not os.path.exists(db_directory):
         try:
            os.makedirs(db_directory, exist_ok=True)
            print(f"Ensured DuckDB directory exists: '{db_directory}'")
         except OSError as e:
            print(f"❌ Error creating DuckDB directory '{db_directory}': {e}. Exiting.")
            traceback.print_exc()
            sys.exit(1)


    # Establish DuckDB Connection
    duckdb_con = None
    try:
        print(f"Attempting to connect to DuckDB database: {db_path}")
        duckdb_con = duckdb.connect(database=db_path, read_only=False)
        print("✅ DuckDB connection successful.")

        # --- Step 1: Get Initial State ---
        print("\n--- Getting Initial Database State ---")
        initial_count, initial_df_head, table_existed_before = get_table_state(duckdb_con, table_name)

        if table_existed_before:
            print(f"Initial row count in '{table_name}': {initial_count}")
            if not initial_df_head.empty:
                 print("Initial first 5 rows:")
                 print(initial_df_head)
            else:
                 print("Initial table is empty or could not fetch rows.")
        else:
            print(f"Table '{table_name}' did not exist before this run.")

        # --- Optional: Drop table before running for a clean demonstration each time ---
        print(f"\n--- Dropping table '{table_name}' if it exists for a clean run ---")
        try:
            duckdb_con.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"Table '{table_name}' dropped if it existed.")
            # Reset initial state after dropping for accurate comparison later
            initial_count = 0
            table_existed_before = False
        except duckdb.Error as e:
            print(f"❌ Error dropping table '{table_name}': {e}")
            # Continue execution, the ETL might still work if the table didn't exist anyway


        # --- Step 2 & 3: Run Extraction, Transformation, and Loading Process (per location) ---
        print("\n--- Running the Extraction, Transformation, and Loading process ---")
        total_loaded_rows = 0
        processed_count = 0
        failed_locations = []

        for location in LOCATIONS_TO_EXTRACT:
            location_name = location.get('name', f"lat{location.get('lat')}_lon{location.get('lon')}")
            print(f"\n--- Processing location: {location_name} ({location.get('lat')},{location.get('lon')}) ---")

            try:
                # 2a. Construct API URL
                api_url = construct_weather_api_url(
                    location_coords=location,
                    api_key=api_key,
                    base_url=base_url
                )

                # 2b. Fetch data from API
                json_data = fetch_data_from_api(api_url, api_timeout)

                if json_data:
                    # 2c. Parse JSON response into DataFrame
                    df = parse_weather_response_to_dataframe(json_data, location)

                    if not df.empty:
                        # 3. Save DataFrame to DuckDB (this will append to the table)
                        # Pass the existing connection to the save function
                        save_weather_to_duckdb(df, db_path, table_name) # save_weather_to_duckdb handles its own connection/closing internally, so we don't pass 'con' directly

                        processed_count += 1
                        total_loaded_rows += len(df)
                        print(f"✅ Successfully processed and loaded data for {location_name}.")
                    else:
                        print(f"Skipping save for {location_name}: No data parsed.")
                        failed_locations.append(location_name)
                else:
                    print(f"Skipping processing for {location_name}: Failed to fetch data.")
                    failed_locations.append(location_name)

            except Exception as e:
                print(f"❌ An error occurred while processing {location_name}: {e}")
                traceback.print_exc()
                failed_locations.append(location_name)

        print(f"\n--- Processing Summary ---")
        print(f"Processed {processed_count} out of {len(LOCATIONS_TO_EXTRACT)} locations.")
        if failed_locations:
            print(f"Failed locations ({len(failed_locations)}): {', '.join(failed_locations)}")
        else:
            print("All locations processed successfully.")


        if total_loaded_rows > 0:
             print(f"\n✅ Overall Loading process completed. Total rows loaded: {total_loaded_rows}.")
             etl_success = True # Indicate overall ETL success if any rows were loaded
        else:
             print("\n⚠️ Overall Loading process completed, but no rows were loaded.")
             etl_success = False # Indicate overall ETL failure if no rows were loaded


        # --- Step 4: Get Final State and Show Difference ---
        print("\n--- Getting Final Database State and Showing Difference ---")
        final_count, final_df_head, table_exists_after = get_table_state(duckdb_con, table_name)

        print(f"\n--- Final Summary ---")
        print(f"Table: '{table_name}'")
        print(f"Row count BEFORE ETL (after potential drop): {initial_count}")
        print(f"Row count AFTER ETL: {final_count if table_exists_after else 'Table does not exist'}")

        # Show the newly added rows based on the timestamp if the table exists after the run
        # Note: The timestamp comparison might need adjustment based on how DuckDB stores/handles timestamps
        if table_exists_after and final_count > initial_count:
             # Pass the timezone-aware run_start_timestamp
             show_newly_added_rows(duckdb_con, table_name, run_start_timestamp)
        elif table_exists_after:
             print("Note: Final row count is not greater than initial, or no new rows found with the current run timestamp.")
        else:
             print("Note: Table did not exist after the run.")


    except duckdb.Error as e:
        print(f"❌ Failed to connect to DuckDB: {e}")
        traceback.print_exc() # Print traceback for connection errors
    except Exception as e:
        print(f"❌ An unexpected error occurred during overall execution: {e}")
        traceback.print_exc() # Print traceback for other errors
    finally:
        if duckdb_con:
            # Commit any pending transactions before closing
            # Note: save_weather_to_duckdb commits internally, but a final commit here is safe
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

