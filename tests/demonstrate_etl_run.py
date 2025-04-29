# demonstrate_etl_run.py - Demonstrates running separated Extract and Load steps

import duckdb
import os
import sys
import pandas as pd
from dotenv import load_dotenv
import datetime 
import traceback   

# --- Path Setup ---
# Get the absolute path of the directory where pytest is being run (project root)
# and add it to sys.path so modules like extract_traffic_duckdb and load_traffic_duckdb can be found.
project_root = os.path.abspath(os.getcwd())
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the necessary functions and CONFIG from your separated scripts
try:
    # Import extract function from extract_traffic_duckdb.py
    from extract_traffic_duckdb import extract_and_transform_traffic_data, CONFIG
    # Import load function from load_traffic_duckdb.py
    from load_traffic_duckdb import load_dataframe_to_duckdb

except ImportError as e:
    print(f"Error: Could not import necessary functions from extract_traffic_duckdb.py or load_traffic_duckdb.py.")
    print(f"Ensure these files exist in the project root ({project_root}) and there are no other import issues.")
    print(f"Details: {e}")
    sys.exit(1) # Exit if the main scripts cannot be imported

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
    Assumes 'extraction_timestamp' column exists and is a datetime type.
    """
    print(f"\n--- Rows added during this run (Timestamp >= {run_timestamp}) ---")
    try:
        # Need to handle potential timezone issues if timestamps are stored with timezone
        # For simplicity, assuming naive timestamps or consistent timezone handling
        query = f"""
        SELECT *
        FROM {table_name}
        WHERE extraction_timestamp >= ?
        """
        # Using parameter binding for the timestamp
        new_rows_df = con.execute(query, [run_timestamp]).fetchdf()

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
    print("Running demonstration script (separated Extract and Load)...")

    # Load environment variables
    load_dotenv()

    # Capture the timestamp before the ETL run starts
    run_start_timestamp = datetime.datetime.now()
    print(f"Run started at: {run_start_timestamp}")


    # Establish DuckDB Connection
    duckdb_con = None
    try:
        print(f"Attempting to connect to DuckDB database: {CONFIG['DUCKDB_DATABASE']}")
        # Ensure the database directory exists if it's not in the current directory
        db_dir = os.path.dirname(CONFIG['DUCKDB_DATABASE'])
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
            print(f"Created database directory: {db_dir}")

        duckdb_con = duckdb.connect(database=CONFIG["DUCKDB_DATABASE"], read_only=False)
        print("✅ DuckDB connection successful.")

        table_name = CONFIG["TRAFFIC_TABLE_NAME"]

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

        # Drop table before running for a clean demonstration each time if needed
        # print(f"\n--- Dropping table '{table_name}' if it exists for a clean run ---")
        # try:
        #     duckdb_con.execute(f"DROP TABLE IF EXISTS {table_name}")
        #     print(f"Table '{table_name}' dropped if it existed.")
        #     # Reset initial state after dropping for accurate comparison later
        #     initial_count = 0
        #     table_existed_before = False
        # except duckdb.Error as e:
        #     print(f"❌ Error dropping table '{table_name}': {e}")
        #     # Continue execution, the ETL might still work if the table didn't exist anyway


        # --- Step 2: Run the Extraction and Transformation Process ---
        print("\n--- Running the Extraction and Transformation process ---")
        extracted_dataframes = extract_and_transform_traffic_data(CONFIG['ROUTE_POINTS_EXAMPLE'])

        if extracted_dataframes:
            print(f"\n✅ Extraction and Transformation completed. Received {len(extracted_dataframes)} DataFrame(s).")
             # --- Step 3: Run the Loading Process ---
            print("\n--- Running the Loading process ---")
            total_loaded_rows = 0
            for i, df_to_load in enumerate(extracted_dataframes):
                point_identifier = CONFIG['ROUTE_POINTS_EXAMPLE'][i] # Assuming order matches points list
                print(f"Loading DataFrame for point: {point_identifier} ({len(df_to_load)} rows)")
                load_dataframe_to_duckdb(duckdb_con, df_to_load, table_name, point_identifier)
                total_loaded_rows += len(df_to_load) # Sum up rows from each loaded DataFrame

            if total_loaded_rows > 0:
                 print(f"\n✅ Loading process completed. Total rows loaded: {total_loaded_rows}.")
                 etl_success = True # Indicate overall ETL success if any rows were loaded
            else:
                 print("\n⚠️ Loading process completed, but no rows were loaded.")
                 etl_success = False # Indicate overall ETL failure if no rows were loaded

        else:
            print("\n⚠️ Extraction and Transformation completed, but no DataFrames were returned. Skipping Load.")
            etl_success = False # Indicate overall ETL failure if no data was extracted


        # --- Step 4: Get Final State and Show Difference ---
        print("\n--- Getting Final Database State and Showing Difference ---")
        final_count, final_df_head, table_exists_after = get_table_state(duckdb_con, table_name)

        print(f"\n--- Summary ---")
        print(f"Table: '{table_name}'")
        print(f"Row count BEFORE ETL (after potential drop): {initial_count}")
        print(f"Row count AFTER ETL: {final_count if table_exists_after else 'Table does not exist'}")

        # Show the newly added rows based on the timestamp if the table exists after the run
        if table_exists_after and final_count > initial_count:
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
            duckdb_con.commit()
            duckdb_con.close()
            print("\n✅ DuckDB connection closed.")

